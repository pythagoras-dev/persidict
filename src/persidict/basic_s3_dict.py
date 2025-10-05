from __future__ import annotations

from typing import Any, Optional
import io

import boto3
import joblib
import jsonpickle
from botocore.exceptions import ClientError

import parameterizable
from parameterizable.dict_sorter import sort_dict_by_keys

from .safe_str_tuple import SafeStrTuple, NonEmptySafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey
from .singletons import (EXECUTION_IS_COMPLETE, ETagHasNotChangedFlag,
                         ETAG_HAS_NOT_CHANGED)


def not_found_error(e:ClientError) -> bool:
    """Helper function to check if a ClientError indicates a missing S3 object.

    Args:
        e: The ClientError exception to check.

    Returns:
        bool: True if the error indicates a missing object (404, NoSuchKey),
        False otherwise.
    """
    status = e.response['ResponseMetadata']['HTTPStatusCode']
    if status == 404:
        return True
    else:
        error_code = e.response['Error']['Code']
        return error_code in ('NoSuchKey', '404', 'NotFound')


class BasicS3Dict(PersiDict):
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
        """Get an ETag for a key."""
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

    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: str | None
                                 ) -> tuple[Any,str|None] | ETagHasNotChangedFlag:
        """Retrieve the value for a key only if its ETag has changed.

        This method is absent in the original dict API.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
            etag: The ETag value to compare against.

        Returns:
            tuple[Any, str|None] | ETagHasNotChangedFlag: The deserialized value
                if the ETag has changed, along with the new ETag,
                or ETAG_HAS_NOT_CHANGED if the etag matches the current one.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)

        try:
            get_kwargs = {'Bucket': self.bucket_name, 'Key': obj_name}
            if etag:
                get_kwargs['IfNoneMatch'] = etag

            response = self.s3_client.get_object(**get_kwargs)

            # 200 OK: object was downloaded, either because it's new or changed.
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
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 304:
                # HTTP 304 Not Modified: the version is current, no download needed
                return ETAG_HAS_NOT_CHANGED
            elif not_found_error(e):
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            else:
                raise


    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        """Retrieve the value stored for a key directly from S3.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            Any: The deserialized value stored for the key.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        return self.get_item_if_etag_changed(key, None)[0]


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

        # Serialize the value directly to S3
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

        response = self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=serialized_data,
            ContentType=content_type
        )
        return response.get("ETag")

    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any):
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


    def get_subdict(self, key:PersiDictKey) -> 'BasicS3Dict':
        """Create a subdictionary scoped to items with the specified prefix.

        Returns an empty subdictionary if no items exist under the prefix.
        If the prefix is empty, the entire dictionary is returned.
        This method is not part of the standard Python dictionary interface.

        Args:
            key: A common prefix (string or sequence of strings or SafeStrTuple)
                used to scope items stored under this dictionary.

        Returns:
            BasicS3Dict: A new BasicS3Dict instance with root_prefix
                extended by the given key, sharing the parent's bucket,
                region, serialization_format, and other configuration settings.
        """

        key = SafeStrTuple(key)
        if len(key):
            key = sign_safe_str_tuple(key, 0)
            full_root_prefix = self.root_prefix + "/".join(key)
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

# parameterizable.register_parameterizable_class(BasicS3Dict)