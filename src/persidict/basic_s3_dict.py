"""Basic S3-backed persistent dictionary.

This module provides `BasicS3Dict`, a concrete implementation of the
`PersiDict` interface that stores each dictionary entry as a separate
object in Amazon S3. It is designed for simple, low-overhead persistence
without local caching, while exposing a familiar mapping-like API.

See individual method docstrings for details on semantics and exceptions.
"""

from __future__ import annotations

from typing import Any, Optional
import io

import boto3
import joblib
import jsonpickle
from botocore.exceptions import ClientError

from mixinforge import sort_dict_by_keys

from .safe_str_tuple import SafeStrTuple, NonEmptySafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey, ValueType
from .jokers_and_status_flags import (EXECUTION_IS_COMPLETE, ETagChangeFlag,
                                      KEEP_CURRENT, DELETE_CURRENT,
                                      Joker, ETagInput, ETAG_UNKNOWN,
                                      ETagConditionFlag, EQUAL_ETAG, DIFFERENT_ETAG)


def not_found_error(e:ClientError) -> bool:
    """Check if a ClientError indicates a missing S3 object.

    Args:
        e: The ClientError exception to check.

    Returns:
        True if the error indicates a missing object (404, NoSuchKey),
        False otherwise.
    """
    status = e.response['ResponseMetadata']['HTTPStatusCode']
    if status == 404:
        return True
    else:
        error_code = e.response['Error']['Code']
        return error_code in ('NoSuchKey', '404', 'NotFound')


class BasicS3Dict(PersiDict[ValueType]):
    """A persistent dictionary that stores key-value pairs as S3 objects.
    
    Each key-value pair is stored as a separate S3 object in the specified bucket.
    
    A key can be either a string (object name without file extension) or a sequence
    of strings representing a hierarchical path (folder structure ending with an
    object name). Values can be instances of any Python type and are serialized
    to S3 objects.
    
    BasicS3Dict supports multiple serialization formats:
    - Binary storage using pickle ('pkl' format)  
    - Human-readable text using jsonpickle ('json' format)
    - Plain text for string values (other formats)
    
    Note:
        Unlike native Python dictionaries, insertion order is not preserved.
        Operations may incur S3 API costs and network latency.
        All operations are performed directly against S3 without local caching.
    """
    region: str
    bucket_name: str
    root_prefix: str

    def __init__(self, bucket_name: str = "my_bucket",
                 region: str = None,
                 root_prefix: str = "",
                 serialization_format: str = "pkl",
                 append_only: bool = False,
                 base_class_for_values: Optional[type] = None,
                 *args, **kwargs):
        """Initialize a basic S3-backed persistent dictionary.

        Args:
            bucket_name: Name of the S3 bucket to use. The bucket will be
                created automatically if it does not exist and permissions allow.
            region: AWS region for the bucket. If None, uses the default
                client region from AWS configuration.
            root_prefix: Common S3 key prefix under which all objects are
                stored. A trailing slash is automatically added if missing.
            serialization_format: File extension/format for stored values. Supported formats:
                'pkl' (pickle), 'json' (jsonpickle), or custom text formats.
            append_only: If True, prevents modification of existing items
                after they are initially stored.
            base_class_for_values: Optional base class that all stored values
                must inherit from. When specified (and not str), serialization_format
                must be 'pkl' or 'json' for proper serialization.
            *args: Additional positional arguments (ignored, reserved for compatibility).
            **kwargs: Additional keyword arguments (ignored, reserved for compatibility).
            
        Note:
            The S3 bucket will be created if it doesn't exist and AWS permissions
            allow. Network connectivity and valid AWS credentials are required.
        """
        
        super().__init__(append_only=append_only,
                         base_class_for_values=base_class_for_values,
                         serialization_format=serialization_format)
        
        self.region = region
        if region is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = boto3.client('s3', region_name=region)

        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if not_found_error(e):
                # Bucket does not exist, attempt to create it
                try:
                    effective_region = self.s3_client.meta.region_name
                    if effective_region and effective_region != 'us-east-1':
                        self.s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': effective_region})
                    else:
                        self.s3_client.create_bucket(Bucket=bucket_name)

                except ClientError as create_e:
                    create_error_code = create_e.response['Error']['Code']
                    # Handle race condition where the bucket was created by another
                    # process or its name is already taken by another AWS account
                    if ( create_error_code == 'BucketAlreadyOwnedByYou'
                        or create_error_code == 'BucketAlreadyExists'):
                        pass
                    else:
                        raise create_e  # Re-raise other unexpected creation errors
            elif error_code == '403' or error_code == 'Forbidden':
                # Bucket exists but access is forbidden - likely a cross-account
                # bucket with policy granting limited access. Operations may still
                # work if the policy allows the required S3 permissions.
                pass
            else:
                raise e  # Re-raise other unexpected head_bucket errors

        self.bucket_name = bucket_name

        self.root_prefix = root_prefix
        if len(self.root_prefix) and self.root_prefix[-1] != "/":
            self.root_prefix += "/"


    def get_params(self):
        """Return configuration parameters as a dictionary.

        This method supports the Parameterizable API and is not part of
        the standard Python dictionary interface.

        Returns:
            dict: A mapping of parameter names to their configured values,
            including S3-specific parameters (region, bucket_name, root_prefix)
            sorted by key names.
        """
        params = {
            "region": self.region,
            "bucket_name": self.bucket_name,
            "root_prefix": self.root_prefix,
            "serialization_format": self.serialization_format,
            "append_only": self.append_only,
            "base_class_for_values": self.base_class_for_values,
        }
        sorted_params = sort_dict_by_keys(params)
        return sorted_params



    def etag(self, key:NonEmptyPersiDictKey) -> str|None:
        """Get an ETag for a key.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).

        Returns:
            str|None: The ETag value for the S3 object, or None if not available.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return response["ETag"]
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            else:
                raise


    @property
    def base_url(self) -> str|None:
        """Return the S3 URL prefix of this dictionary.

        This property is not part of the standard Python dictionary interface.

        Returns:
            str: The base S3 URL in the format "s3://<bucket>/<root_prefix>".
        """
        return f"s3://{self.bucket_name}/{self.root_prefix}"


    def _build_full_objectname(self, key: NonEmptyPersiDictKey) -> str:
        """Convert a key into a full S3 object key.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            str: The complete S3 object key including root_prefix and serialization_format
            extension, with digest-based collision prevention applied if enabled.
        """
        key = NonEmptySafeStrTuple(key)
        key = sign_safe_str_tuple(key, 0)
        objectname = self.root_prefix + "/".join(key) + "." + self.serialization_format
        return objectname


    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Check if the specified key exists in the dictionary.

        Performs a HEAD request to S3 to verify the object's existence.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            bool: True if the key exists in S3, False otherwise.
        """
        key = NonEmptySafeStrTuple(key)
        try:
            obj_name = self._build_full_objectname(key)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return True
        except ClientError as e:
            if not_found_error(e):
                return False
            else:
                raise

    def get_item_if_etag(
            self,
            key: NonEmptyPersiDictKey,
            etag: ETagInput,
            condition: ETagConditionFlag
    ) -> tuple[ValueType, str | None] | ETagChangeFlag:
        """Retrieve the value for a key only if its ETag satisfies a condition.

        This method is absent in the original dict API.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
            etag: The ETag value to compare against, or ETAG_UNKNOWN if unset.
            condition: EQUAL_ETAG to require a match, or DIFFERENT_ETAG to
                require a mismatch.

        Returns:
            tuple[Any, str|None] | ETagChangeFlag:
                The deserialized value if the condition succeeds, along with
                the current ETag, or a sentinel flag if the condition fails.

        Raises:
            KeyError: If the key does not exist in S3.
            ValueError: If condition is not EQUAL_ETAG or DIFFERENT_ETAG.
        """
        etag = self._normalize_etag_input(etag)
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        failure_flag = self._etag_condition_failure_flag(condition)

        try:
            get_kwargs = {'Bucket': self.bucket_name, 'Key': obj_name}
            if condition is DIFFERENT_ETAG:
                if etag is not ETAG_UNKNOWN:
                    get_kwargs['IfNoneMatch'] = etag
            elif condition is EQUAL_ETAG:
                if etag is ETAG_UNKNOWN:
                    # Preserve KeyError for missing keys while matching prior behavior.
                    self.etag(key)
                    return failure_flag
                get_kwargs['IfMatch'] = etag
            else:
                raise ValueError("condition must be EQUAL_ETAG or DIFFERENT_ETAG")

            response = self.s3_client.get_object(**get_kwargs)

            # 200 OK: object was downloaded, either because it's new or matches.
            body = response['Body']
            s3_etag = response.get("ETag")

            try:
                if self.serialization_format == 'json':
                    deserialized_value = jsonpickle.loads(body.read().decode('utf-8'))
                elif self.serialization_format == 'pkl':
                    with io.BytesIO(body.read()) as buffer:
                        deserialized_value = joblib.load(buffer)
                else:
                    deserialized_value = body.read().decode('utf-8')
            finally:
                body.close()

            return (deserialized_value, s3_etag)

        except ClientError as e:
            status = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            code = e.response.get('Error', {}).get('Code')
            if condition is DIFFERENT_ETAG and status == 304:
                # HTTP 304 Not Modified: the version is current, no download needed.
                return failure_flag
            if condition is EQUAL_ETAG and (status in (409, 412)
                                            or code in ("ConditionalRequestConflict", "PreconditionFailed")):
                return failure_flag
            if not_found_error(e):
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            raise


    def __getitem__(self, key: NonEmptyPersiDictKey) -> ValueType:
        """Retrieve the value stored for a key directly from S3.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            Any: The deserialized value stored for the key.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        return self.get_item_if_etag(key, ETAG_UNKNOWN, DIFFERENT_ETAG)[0]

    def setdefault(self, key: NonEmptyPersiDictKey, default: ValueType | None = None) -> ValueType:
        """Insert key with default value if absent; return the current value.

        Uses an S3 conditional put (If-None-Match: ``*``) to avoid overwriting
        existing values under concurrent writers. On conditional failure,
        returns the current value without modifying it.

        Args:
            key: Key (string, sequence of strings, or SafeStrTuple).
            default: Value to insert if the key is not present. Defaults to None.

        Returns:
            Existing value if key is present; otherwise the provided default value.

        Raises:
            TypeError: If default is a Joker command (KEEP_CURRENT/DELETE_CURRENT),
                or if the key is missing and default violates value type constraints.
        """
        key = NonEmptySafeStrTuple(key)
        if isinstance(default, Joker):
            raise TypeError("default must be a regular value, not a Joker command")

        invalid_default = isinstance(default, PersiDict)
        if not invalid_default and self.base_class_for_values is not None:
            invalid_default = not isinstance(default, self.base_class_for_values)

        if invalid_default:
            try:
                return self[key]
            except KeyError:
                if isinstance(default, PersiDict):
                    raise TypeError("Cannot store a PersiDict instance directly")
                raise TypeError(f"Value must be an instance of"
                                f" {self.base_class_for_values.__name__}")

        try:
            serialized_data, content_type = self._serialize_value_for_s3(default)
        except Exception as exc:
            try:
                return self[key]
            except KeyError:
                raise exc from None
        obj_name = self._build_full_objectname(key)

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=serialized_data,
                ContentType=content_type,
                IfNoneMatch="*"
            )
            return default
        except ClientError as e:
            status = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            code = e.response.get('Error', {}).get('Code')
            if status in (409, 412) or code in ("ConditionalRequestConflict", "PreconditionFailed"):
                return self[key]
            raise


    def _serialize_value_for_s3(self, value: Any) -> tuple[bytes, str]:
        """Serialize a value for S3 storage and return (bytes, content_type)."""
        if self.serialization_format == 'json':
            serialized_data = jsonpickle.dumps(value, indent=4).encode('utf-8')
            content_type = 'application/json'
        elif self.serialization_format == 'pkl':
            with io.BytesIO() as buffer:
                joblib.dump(value, buffer)
                serialized_data = buffer.getvalue()
            content_type = 'application/octet-stream'
        else:
            if isinstance(value, str):
                serialized_data = value.encode('utf-8')
            else:
                serialized_data = str(value).encode('utf-8')
            content_type = 'text/plain'
        return serialized_data, content_type


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> str|None:
        """Store a value for a key directly in S3 and return the new ETag.

        Handles special joker values (KEEP_CURRENT, DELETE_CURRENT) for
        conditional operations. Validates value types against base_class_for_values
        if specified, then serializes and uploads directly to S3.

        This method is absent in the original dict API.

        Args:
            key: Dictionary key (string or sequence of strings)
                or NonEmptySafeStrTuple.
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Returns:
            str|None: The ETag of the newly stored object, or None if a joker
            command was processed without uploading a new object.

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True.
            TypeError: If value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """

        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None

        obj_name = self._build_full_objectname(key)

        serialized_data, content_type = self._serialize_value_for_s3(value)

        response = self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=serialized_data,
            ContentType=content_type
        )
        return response.get("ETag")


    def set_item_if_etag(
            self,
            key: NonEmptyPersiDictKey,
            value: Any,
            etag: ETagInput,
            condition: ETagConditionFlag
    ) -> str | None | ETagChangeFlag:
        """Store a value only if the ETag satisfies a condition.

        For EQUAL_ETAG, uses conditional S3 writes (If-Match) to avoid
        overwriting changes made by other writers; the ETag check is enforced
        server-side. For DIFFERENT_ETAG, performs an ETag comparison and then
        writes without a conditional header (last-write-wins semantics under
        concurrency).

        Args:
            key: Dictionary key (string or sequence of strings)
                or NonEmptySafeStrTuple.
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).
            etag: The ETag value to compare against, or ETAG_UNKNOWN if unset.
            condition: EQUAL_ETAG to require a match, or DIFFERENT_ETAG to
                require a mismatch.

        Returns:
            str | None | ETagChangeFlag: The ETag
                of the newly stored object if the condition succeeds, or a
                sentinel flag if the condition fails.

        Raises:
            KeyError: If the key does not exist.
            ValueError: If condition is not EQUAL_ETAG or DIFFERENT_ETAG.
        """
        etag = self._normalize_etag_input(etag)
        key = NonEmptySafeStrTuple(key)
        failure_flag = self._etag_condition_failure_flag(condition)

        if condition is EQUAL_ETAG:
            if etag is ETAG_UNKNOWN:
                # Preserve KeyError for missing keys while matching prior behavior.
                self.etag(key)
                return failure_flag

            self._validate_setitem_args(key, value)
            if value is KEEP_CURRENT:
                current_etag = self.etag(key)
                if etag != current_etag:
                    return failure_flag
                return None
            if value is DELETE_CURRENT:
                return self.delete_item_if_etag(key, etag, condition)
            serialized_data, content_type = self._serialize_value_for_s3(value)

            try:
                response = self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self._build_full_objectname(key),
                    Body=serialized_data,
                    ContentType=content_type,
                    IfMatch=etag
                )
                return response.get("ETag")
            except ClientError as e:
                status = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                code = e.response.get('Error', {}).get('Code')
                if status in (409, 412) or code in ("ConditionalRequestConflict", "PreconditionFailed"):
                    # Precondition failed (ETag did not match).
                    return failure_flag
                if not_found_error(e):
                    raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
                raise

        if condition is DIFFERENT_ETAG:
            current_etag = self.etag(key)
            if etag == current_etag:
                return failure_flag

            if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
                return None
            serialized_data, content_type = self._serialize_value_for_s3(value)

            try:
                response = self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self._build_full_objectname(key),
                    Body=serialized_data,
                    ContentType=content_type
                )
                return response.get("ETag")
            except ClientError as e:
                if not_found_error(e):
                    raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
                raise

        raise ValueError("condition must be EQUAL_ETAG or DIFFERENT_ETAG")


    def delete_item_if_etag(
            self,
            key: NonEmptyPersiDictKey,
            etag: ETagInput,
            condition: ETagConditionFlag
    ) -> None | ETagChangeFlag:
        """Delete a key only if its ETag satisfies a condition.

        For EQUAL_ETAG, uses conditional S3 deletes (If-Match) to avoid deleting
        objects modified by other writers between the check and the delete. For
        DIFFERENT_ETAG, falls back to the non-atomic base implementation (S3
        does not support an If-None-Match delete condition).
        """
        etag = self._normalize_etag_input(etag)
        key = NonEmptySafeStrTuple(key)
        failure_flag = self._etag_condition_failure_flag(condition)

        if condition is EQUAL_ETAG:
            if etag is ETAG_UNKNOWN:
                # Preserve KeyError for missing keys while matching prior behavior.
                self.etag(key)
                return failure_flag
            current_etag = self.etag(key)
            if etag != current_etag:
                return failure_flag

            try:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=self._build_full_objectname(key),
                    IfMatch=etag
                )
                return None
            except ClientError as e:
                status = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                code = e.response.get('Error', {}).get('Code')
                if status in (409, 412) or code in ("ConditionalRequestConflict", "PreconditionFailed"):
                    return failure_flag
                if not_found_error(e):
                    raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
                raise

        if condition is DIFFERENT_ETAG:
            return super().delete_item_if_etag(key, etag, condition)

        raise ValueError("condition must be EQUAL_ETAG or DIFFERENT_ETAG")


    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType) -> None:
        """Store a value for a key directly in S3.

        Handles special joker values (KEEP_CURRENT, DELETE_CURRENT) for
        conditional operations. Validates value types against base_class_for_values
        if specified, then serializes and uploads directly to S3.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptyPersiDictKey).
            value: Value to store, or a joker command (KEEP_CURRENT or 
                DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True.
            TypeError: If value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """
        self.set_item_get_etag(key, value)


    def __delitem__(self, key: NonEmptyPersiDictKey):
        """Delete the stored value for a key from S3.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptyPersiDictKey).

        Raises:
            KeyError: If append_only is True, or if the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        self._process_delitem_args(key)
        obj_name = self._build_full_objectname(key)
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj_name)
        except ClientError as e:
            if not_found_error(e):
                pass
            else:
                raise

    def __len__(self) -> int:
        """Return the number of key-value pairs in the dictionary.

        Warning:
            This operation can be very slow and expensive on large S3 buckets
            as it must paginate through all objects under the dictionary's prefix.
            Avoid using in performance-critical code.

        Returns:
            int: Number of stored items under this dictionary's root_prefix.
        """

        num_files = 0
        suffix = "." + self.serialization_format

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name, Prefix=self.root_prefix)

        for page in page_iterator:
            contents = page.get("Contents")
            if not contents:
                continue
            for key in contents:
                obj_name = key["Key"]
                if obj_name.endswith(suffix):
                    num_files += 1

        return num_files


    def _generic_iter(self, result_type: set[str]):
        """Underlying implementation for items(), keys(), and values() iterators.

        Paginates through S3 objects under the configured root_prefix and yields
        keys, values, and/or timestamps according to the requested result_type.
        S3 object keys are converted to SafeStrTuple instances by removing the
        file extension and reversing digest-based signing if enabled.

        Args:
            result_type: Non-empty subset of {"keys", "values", "timestamps"}
                specifying which fields to yield from each dictionary entry.

        Returns:
            Iterator: A generator that yields:
                - SafeStrTuple if result_type == {"keys"}
                - Any if result_type == {"values"}  
                - tuple[SafeStrTuple, Any] if result_type == {"keys", "values"}
                - tuple including float timestamp if "timestamps" requested

        Raises:
            ValueError: If result_type is invalid (empty, not a set, or contains
                unsupported field names).
        """

        self._process_generic_iter_args(result_type)

        suffix = "." + self.serialization_format
        ext_len = len(self.serialization_format) + 1
        prefix_len = len(self.root_prefix)

        def splitter(full_name: str) -> SafeStrTuple:
            """Convert an S3 object key into a SafeStrTuple without the file extension.

            Args:
                full_name: Complete S3 object key including root_prefix and extension.

            Returns:
                SafeStrTuple: The parsed key components with digest signatures intact.

            Raises:
                ValueError: If the object key does not start with this dictionary's
                    root_prefix (indicating it's outside the dictionary's scope).
            """
            if not full_name.startswith(self.root_prefix):
                raise ValueError(
                    f"S3 object key '{full_name}' is outside of root_prefix '{self.root_prefix}'"
                )
            result = full_name[prefix_len:-ext_len].split(sep="/")
            return SafeStrTuple(result)

        def step():
            """Generator that paginates through S3 objects and yields requested data.
            
            Yields dictionary entries (keys, values, timestamps) according to the
            result_type specification from the parent _generic_iter method.
            """
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name, Prefix=self.root_prefix)

            for page in page_iterator:
                contents = page.get("Contents")
                if not contents:
                    continue
                for key in contents:
                    obj_name = key["Key"]
                    if not obj_name.endswith(suffix):
                        continue
                    obj_key = splitter(obj_name)

                    to_return = []
                    unsigned_key = unsign_safe_str_tuple(
                        obj_key, 0)

                    if "keys" in result_type:
                        to_return.append(unsigned_key)

                    if "values" in result_type:
                        # The object can be deleted between listing and fetching.
                        # Skip such races instead of raising to make iteration robust.
                        try:
                            value_to_return = self[unsigned_key]
                        except KeyError:
                            continue
                        to_return.append(value_to_return)

                    if len(result_type) == 1:
                        yield to_return[0]
                    else:
                        if "timestamps" in result_type:
                            timestamp_to_return = key["LastModified"].timestamp()
                            to_return.append(timestamp_to_return)
                        yield tuple(to_return)

        return step()


    def get_subdict(self, prefix_key:PersiDictKey) -> 'BasicS3Dict[ValueType]':
        """Create a subdictionary scoped to items with the specified prefix.

        Returns an empty subdictionary if no items exist under the prefix.
        If the prefix is empty, the entire dictionary is returned.
        This method is not part of the standard Python dictionary interface.

        Args:
            prefix_key: A common prefix (string or sequence of strings or SafeStrTuple)
                used to scope items stored under this dictionary.

        Returns:
            BasicS3Dict: A new BasicS3Dict instance with root_prefix
                extended by the given prefix_key, sharing the parent's bucket,
                region, serialization_format, and other configuration settings.
        """

        prefix_key = SafeStrTuple(prefix_key)
        if len(prefix_key):
            prefix_key = sign_safe_str_tuple(prefix_key, 0)
            full_root_prefix = self.root_prefix + "/".join(prefix_key)
        else:
            full_root_prefix = self.root_prefix

        new_dict = BasicS3Dict(
            bucket_name=self.bucket_name,
            region=self.region,
            root_prefix=full_root_prefix,
            serialization_format=self.serialization_format,
            append_only=self.append_only,
            base_class_for_values=self.base_class_for_values)

        return new_dict


    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Get the last modification timestamp for a key.

        This method is not part of the standard Python dictionary interface.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            float: POSIX timestamp (seconds since Unix epoch) of the last
            modification time as reported by S3. The timestamp is timezone-aware
            and converted to UTC.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return response["LastModified"].timestamp()
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            else:
                raise
