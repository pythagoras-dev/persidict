"""Basic S3-backed persistent dictionary.

This module provides `BasicS3Dict`, a concrete implementation of the
`PersiDict` interface that stores each dictionary entry as a separate
object in Amazon S3. It is designed for simple, low-overhead persistence
without local caching, while exposing a familiar mapping-like API.

See individual method docstrings for details on semantics and exceptions.
"""

from __future__ import annotations

from typing import Any, Final
import io

import boto3
from botocore.exceptions import ClientError

from mixinforge import sort_dict_by_keys

from .safe_str_tuple import SafeStrTuple, NonEmptySafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey, ValueType
from .exceptions import MutationPolicyError, ConcurrencyConflictError
from .jokers_and_status_flags import (EXECUTION_IS_COMPLETE,
                                      KEEP_CURRENT, DELETE_CURRENT,
                                      Joker, ETagValue,
                                      ETagConditionFlag,
                                      ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED,
                                      RetrieveValueFlag, ALWAYS_RETRIEVE,
                                      NEVER_RETRIEVE, IF_ETAG_CHANGED,
                                      ITEM_NOT_AVAILABLE, ItemNotAvailableFlag,
                                      VALUE_NOT_RETRIEVED,
                                      ETagIfExists, ConditionalOperationResult)


_MAX_SETDEFAULT_RETRIES: Final[int] = 5


def _s3_error_status_code(e: ClientError) -> tuple[int | None, str | None]:
    """Return HTTP status and S3 error code if present on a ClientError."""
    response = getattr(e, "response", {}) or {}
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = response.get("Error", {}).get("Code")
    return status, code


def not_found_error(e: ClientError) -> bool:
    """Check if a ClientError indicates a missing S3 object.

    Args:
        e: The ClientError exception to check.

    Returns:
        True if the error indicates a missing object (404, NoSuchKey),
        False otherwise.
    """
    status, error_code = _s3_error_status_code(e)
    if status == 404:
        return True
    return error_code in ("NoSuchKey", "404", "NotFound")


def conditional_request_failed(e: ClientError) -> bool:
    """Check if a ClientError indicates a failed conditional request."""
    status, code = _s3_error_status_code(e)
    return status in (409, 412) or code in ("ConditionalRequestConflict", "PreconditionFailed")


def not_modified_error(e: ClientError) -> bool:
    """Check if a ClientError indicates an If-None-Match not modified response."""
    status, code = _s3_error_status_code(e)
    return status == 304 or code in ("304", "NotModified")


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
    _conditional_delete_probed: bool = False
    _conditional_delete_supported: bool = True
    _if_none_match_etag_probed: bool = False
    _if_none_match_etag_supported: bool = True

    _CONTENT_TYPE_MAP: dict[str, str] = {
        'json': 'application/json',
        'pkl': 'application/octet-stream',
    }

    def __init__(self, *,
                 bucket_name: str = "my_bucket",
                 region: str = None,
                 root_prefix: str = "",
                 serialization_format: str = "pkl",
                 append_only: bool = False,
                 base_class_for_values: type | None = None):
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


    def get_params(self) -> dict[str, Any]:
        """Return configuration parameters as a dictionary.

        This method supports the Parameterizable API and is not part of
        the standard Python dictionary interface.

        Returns:
            A mapping of parameter names to their configured values,
            including S3-specific parameters (region, bucket_name, root_prefix)
            sorted by key names.
        """
        params = super().get_params()
        additional_params = dict(
            region=self.region,
            bucket_name=self.bucket_name,
            root_prefix=self.root_prefix)
        params = {**params, **additional_params}
        sorted_params = sort_dict_by_keys(params)
        return sorted_params



    def etag(self, key: NonEmptyPersiDictKey) -> ETagValue:
        """Get an ETag for a key.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).

        Returns:
            The ETag value for the S3 object.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return ETagValue(response["ETag"])
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(key) from e
            else:
                raise


    @property
    def base_url(self) -> str|None:
        """Return the S3 URL prefix of this dictionary.

        This property is not part of the standard Python dictionary interface.

        Returns:
            The base S3 URL in the format "s3://<bucket>/<root_prefix>".
        """
        return f"s3://{self.bucket_name}/{self.root_prefix}"


    def _build_full_objectname(self, key: NonEmptyPersiDictKey) -> str:
        """Convert a key into a full S3 object key.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            The complete S3 object key including root_prefix and serialization_format
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
            True if the key exists in S3, False otherwise.
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

    def _deserialize_s3_body(self, body) -> Any:
        """Deserialize a value from an S3 response body.

        Args:
            body: The S3 response Body stream.

        Returns:
            The deserialized value.
        """
        try:
            raw = body.read()
            if self.serialization_format == 'pkl':
                f = io.BytesIO(raw)
            else:
                f = io.StringIO(raw.decode('utf-8'))
            return self._deserialize_from_file(f)
        finally:
            body.close()

    def _get_value_and_etag(self, key: NonEmptySafeStrTuple) -> tuple[ValueType, ETagValue]:
        """Return the value and ETag for a key in a single S3 request.

        Args:
            key: Normalized dictionary key.

        Returns:
            The deserialized value and ETag.

        Raises:
            KeyError: If the key does not exist in S3.
            TypeError: If base_class_for_values is set and the stored value
                does not match it.
        """
        obj_name = self._build_full_objectname(key)
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=obj_name)
            s3_etag = ETagValue(response["ETag"])
            value = self._deserialize_s3_body(response['Body'])
            self._validate_returned_value(value)
            return value, s3_etag
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(key) from e
            raise

    def _result_for_missing_key(
            self,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists
    ) -> ConditionalOperationResult[ValueType]:
        """Build a ConditionalOperationResult for a missing key."""
        satisfied = self._check_condition(condition, expected_etag, ITEM_NOT_AVAILABLE)
        return self._result_item_not_available(condition, satisfied)

    def _conditional_failure_result(
            self,
            key: NonEmptySafeStrTuple,
            condition: ETagConditionFlag,
            *,
            expected_etag: ETagIfExists = ITEM_NOT_AVAILABLE,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED,
            return_existing_value: bool = True
    ) -> ConditionalOperationResult[ValueType]:
        """Build result for a failed conditional write/delete."""
        if not return_existing_value or retrieve_value is NEVER_RETRIEVE:
            new_actual = self._actual_etag(key)
            if new_actual is ITEM_NOT_AVAILABLE:
                return self._result_item_not_available(condition, False)
            return self._result_unchanged(
                condition, False, new_actual, VALUE_NOT_RETRIEVED)

        if retrieve_value is ALWAYS_RETRIEVE:
            try:
                new_value, new_actual = self._get_value_and_etag(key)
            except KeyError:
                return self._result_item_not_available(condition, False)
            return self._result_unchanged(
                condition, False, new_actual, new_value)

        # IF_ETAG_CHANGED: fetch only if actual ETag differs from expected
        new_actual = self._actual_etag(key)
        if new_actual is ITEM_NOT_AVAILABLE:
            return self._result_item_not_available(condition, False)
        if expected_etag != new_actual:
            try:
                new_value, new_actual = self._get_value_and_etag(key)
            except KeyError:
                return self._result_item_not_available(condition, False)
            return self._result_unchanged(
                condition, False, new_actual, new_value)
        return self._result_unchanged(
            condition, False, new_actual, VALUE_NOT_RETRIEVED)

    def get_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Retrieve the value for a key only if an ETag condition is satisfied.

        Uses S3 conditional headers (IfMatch/IfNoneMatch) for server-side
        condition checking when possible.

        Args:
            key: Dictionary key.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE.
            retrieve_value: Controls value retrieval. IF_ETAG_CHANGED
                (default) uses S3 IfNoneMatch to skip the fetch when the
                ETag matches. ALWAYS_RETRIEVE always fetches the value.
                NEVER_RETRIEVE does a HEAD only and returns
                VALUE_NOT_RETRIEVED.

        Returns:
            ConditionalOperationResult with the outcome.

        Raises:
            TypeError: If base_class_for_values is set and the retrieved value
                does not match it.
        """
        self._validate_retrieve_value(retrieve_value)
        key = NonEmptySafeStrTuple(key)
        if retrieve_value is ALWAYS_RETRIEVE:
            try:
                value, actual_etag = self._get_value_and_etag(key)
            except KeyError:
                return self._result_for_missing_key(condition, expected_etag)

            satisfied = self._check_condition(condition, expected_etag, actual_etag)
            return self._result_unchanged(condition, satisfied, actual_etag, value)

        if retrieve_value is NEVER_RETRIEVE:
            actual_etag = self._actual_etag(key)
            if actual_etag is ITEM_NOT_AVAILABLE:
                return self._result_for_missing_key(condition, expected_etag)
            satisfied = self._check_condition(condition, expected_etag, actual_etag)
            return self._result_unchanged(
                condition, satisfied, actual_etag, VALUE_NOT_RETRIEVED)

        obj_name = self._build_full_objectname(key)
        use_if_none_match = not isinstance(expected_etag, ItemNotAvailableFlag)
        get_kwargs = {
            "Bucket": self.bucket_name,
            "Key": obj_name
        }
        if use_if_none_match:
            get_kwargs["IfNoneMatch"] = expected_etag

        try:
            response = self.s3_client.get_object(**get_kwargs)
            actual_etag = ETagValue(response["ETag"])
            value = self._deserialize_s3_body(response["Body"])
        except ClientError as e:
            if not_found_error(e):
                return self._result_for_missing_key(condition, expected_etag)
            if use_if_none_match and not_modified_error(e):
                satisfied = self._check_condition(
                    condition, expected_etag, expected_etag)
                return self._result_unchanged(
                    condition, satisfied, expected_etag, VALUE_NOT_RETRIEVED)
            raise

        self._validate_returned_value(value)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)
        return self._result_unchanged(condition, satisfied, actual_etag, value)

    def __getitem__(self, key: NonEmptyPersiDictKey) -> ValueType:
        """Retrieve the value stored for a key directly from S3.

        Args:
            key: Dictionary key (string or sequence of strings
            or NonEmptySafeStrTuple).

        Returns:
            The deserialized value stored for the key.

        Raises:
            KeyError: If the key does not exist in S3.
            TypeError: If base_class_for_values is set and the stored value
                does not match it.
        """
        key = NonEmptySafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=obj_name)
            value = self._deserialize_s3_body(response['Body'])
            self._validate_returned_value(value)
            return value
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(key) from e
            raise

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
            ConcurrencyConflictError: If retries are exhausted due to
                concurrent modifications.
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

        for _ in range(_MAX_SETDEFAULT_RETRIES):
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
                if conditional_request_failed(e):
                    try:
                        return self[key]
                    except KeyError:
                        # Key was deleted between our failed put and
                        # our read — retry the whole operation.
                        continue
                raise

        raise ConcurrencyConflictError(key, _MAX_SETDEFAULT_RETRIES)


    def _serialize_value_for_s3(self, value: Any) -> tuple[bytes, str]:
        """Serialize a value for S3 storage and return (bytes, content_type)."""
        content_type = self._CONTENT_TYPE_MAP.get(
            self.serialization_format, 'text/plain')
        if self.serialization_format == 'pkl':
            with io.BytesIO() as buffer:
                self._serialize_to_file(value, buffer)
                serialized_data = buffer.getvalue()
        else:
            with io.StringIO() as buffer:
                self._serialize_to_file(value, buffer)
                serialized_data = buffer.getvalue().encode('utf-8')
        return serialized_data, content_type

    @staticmethod
    def _compute_conditional_headers(
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            actual_etag: ETagIfExists = ITEM_NOT_AVAILABLE
    ) -> tuple[str | None, str | None]:
        """Map a condition and expected ETag to S3 conditional headers.

        Args:
            condition: The ETag condition to enforce.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE.
            actual_etag: The actual ETag (needed for ETAG_HAS_CHANGED with
                ITEM_NOT_AVAILABLE expected_etag).

        Returns:
            (if_match, if_none_match) for use in S3 put/delete calls.
        """
        if condition is ETAG_IS_THE_SAME:
            if isinstance(expected_etag, ItemNotAvailableFlag):
                return None, "*"
            return expected_etag, None
        if condition is ETAG_HAS_CHANGED:
            if isinstance(expected_etag, ItemNotAvailableFlag):
                return actual_etag, None
            return None, expected_etag
        return None, None

    def _put_object_with_conditions(
            self,
            key: NonEmptySafeStrTuple,
            value: Any,
            *,
            if_match: str | None = None,
            if_none_match: str | None = None
    ) -> ETagValue:
        """Serialize and upload a value to S3, returning the new ETag."""
        obj_name = self._build_full_objectname(key)
        serialized_data, content_type = self._serialize_value_for_s3(value)
        put_kwargs = {
            "Bucket": self.bucket_name,
            "Key": obj_name,
            "Body": serialized_data,
            "ContentType": content_type,
        }
        if if_match is not None:
            put_kwargs["IfMatch"] = if_match
        if if_none_match is not None:
            put_kwargs["IfNoneMatch"] = if_none_match
        response = self.s3_client.put_object(**put_kwargs)
        return ETagValue(response["ETag"])


    def _put_object_get_etag(self, key: NonEmptySafeStrTuple, value: Any) -> ETagValue:
        """Serialize and upload a value to S3, returning the new ETag.

        Args:
            key: Normalized dictionary key.
            value: Value to store (not a joker).

        Returns:
            The ETag of the newly stored object.
        """
        return self._put_object_with_conditions(key, value)

    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            value: ValueType | Joker,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Store a value only if an ETag condition is satisfied.

        Uses S3 conditional headers (IfMatch / IfNoneMatch) for a
        single-roundtrip put when the condition can be fully expressed
        without a prior HEAD.  This covers ETAG_IS_THE_SAME (any
        expected_etag) and ETAG_HAS_CHANGED with a real expected_etag.
        Other combinations fall back to check-then-write.

        Args:
            key: Dictionary key.
            value: Value to store.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE.
            retrieve_value: Controls value retrieval on condition failure.
                IF_ETAG_CHANGED (default) fetches only if
                expected_etag != actual_etag. ALWAYS_RETRIEVE fetches
                the existing value. NEVER_RETRIEVE returns
                VALUE_NOT_RETRIEVED.

        Returns:
            ConditionalOperationResult with the outcome.
        """
        self._validate_retrieve_value(retrieve_value)
        key = NonEmptySafeStrTuple(key)
        if self.append_only and value is DELETE_CURRENT:
            raise MutationPolicyError("append-only")
        self._validate_value(value)

        # Fast path: single-roundtrip S3 conditional put using
        # IfMatch / IfNoneMatch headers.  Eligible when we can fully
        # express the condition in S3 headers without a prior HEAD:
        #   - ETAG_IS_THE_SAME (any expected_etag), unless append_only
        #     (except insert-only: ETAG_IS_THE_SAME + ITEM_NOT_AVAILABLE,
        #      which maps to IfNoneMatch:* and is safe in append_only)
        #   - ETAG_HAS_CHANGED with a real expected_etag, provided
        #     the backend enforces IfNoneMatch with specific ETags
        #     (ITEM_NOT_AVAILABLE needs actual_etag for IfMatch,
        #      so it must go through the fallback HEAD path)
        _fast_eligible = (
            (not self.append_only
             or (condition is ETAG_IS_THE_SAME
                 and isinstance(expected_etag, ItemNotAvailableFlag)))
            and value is not KEEP_CURRENT
            and value is not DELETE_CURRENT
            and condition is ETAG_IS_THE_SAME
        )
        if not _fast_eligible and (
            not self.append_only
            and value is not KEEP_CURRENT
            and value is not DELETE_CURRENT
            and condition is ETAG_HAS_CHANGED
            and not isinstance(expected_etag, ItemNotAvailableFlag)
        ):
            if not type(self)._if_none_match_etag_probed:
                self._probe_if_none_match_etag()
            if type(self)._if_none_match_etag_supported:
                _fast_eligible = True

        if _fast_eligible:
            return self._set_item_if_fast_path(
                key, value, condition, expected_etag, retrieve_value)

        return self._set_item_if_fallback(
            key, value, condition, expected_etag, retrieve_value)

    def _set_item_if_fast_path(
            self,
            key: NonEmptySafeStrTuple,
            value: ValueType,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag
    ) -> ConditionalOperationResult[ValueType]:
        """Optimistic S3 conditional write in a single round-trip.

        Supports ETAG_IS_THE_SAME (any expected_etag) and
        ETAG_HAS_CHANGED (with a real expected_etag).  Attempts a
        single S3 put with IfMatch/IfNoneMatch headers, avoiding a
        separate ETag check round-trip.

        For ETAG_IS_THE_SAME the reported actual_etag equals
        expected_etag (the PUT confirms it matched).  For
        ETAG_HAS_CHANGED the true pre-write ETag is unknown (S3 does
        not return it); actual_etag is set to expected_etag as the
        caller's last-known reference.
        """
        if_match, if_none_match = self._compute_conditional_headers(
            condition, expected_etag)
        # Best available pre-write ETag: for ETAG_IS_THE_SAME a
        # successful PUT confirms the object had this ETag; for
        # ETAG_HAS_CHANGED the true value is unknown, so we report
        # the caller's last-known ETag as a stable reference.
        actual_etag = (ITEM_NOT_AVAILABLE
                       if isinstance(expected_etag, ItemNotAvailableFlag)
                       else expected_etag)
        try:
            resulting_etag = self._put_object_with_conditions(
                key, value,
                if_match=if_match, if_none_match=if_none_match)
            return self._result_write_success(
                condition, actual_etag, resulting_etag, value)
        except ClientError as e:
            if not_found_error(e):
                return self._result_for_missing_key(condition, expected_etag)
            if not conditional_request_failed(e):
                raise
            return self._conditional_failure_result(
                key, condition,
                expected_etag=expected_etag,
                retrieve_value=retrieve_value,
                return_existing_value=True)

    def _set_item_if_fallback(
            self,
            key: NonEmptySafeStrTuple,
            value: ValueType,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag
    ) -> ConditionalOperationResult[ValueType]:
        """Check-then-act path for conditions other than the fast path.

        Handles jokers (KEEP_CURRENT, DELETE_CURRENT), append_only checks,
        ETAG_HAS_CHANGED, and ANY_ETAG conditions.
        """
        actual_etag = self._actual_etag(key)
        if self.append_only and value is not KEEP_CURRENT:
            if actual_etag is not ITEM_NOT_AVAILABLE:
                raise MutationPolicyError("append-only")

        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if not satisfied:
            if actual_etag is ITEM_NOT_AVAILABLE:
                return self._result_item_not_available(condition, False)
            if retrieve_value is NEVER_RETRIEVE or (
                    retrieve_value is IF_ETAG_CHANGED
                    and expected_etag == actual_etag):
                existing_value = VALUE_NOT_RETRIEVED
            else:
                existing_value = self[key]
            return self._result_unchanged(
                condition, False, actual_etag, existing_value)

        # Handle joker values before attempting S3 write
        if value is KEEP_CURRENT:
            return self._result_unchanged(
                condition, True, actual_etag, VALUE_NOT_RETRIEVED)
        if value is DELETE_CURRENT:
            if actual_etag is ITEM_NOT_AVAILABLE:
                return self._result_item_not_available(condition, satisfied)
            if not type(self)._conditional_delete_probed:
                self._probe_conditional_delete()
            if type(self)._conditional_delete_supported:
                obj_name = self._build_full_objectname(key)
                try:
                    self.s3_client.delete_object(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        IfMatch=actual_etag)
                    return self._result_delete_success(condition, actual_etag)
                except ClientError as e:
                    if not_found_error(e) or conditional_request_failed(e):
                        return self._conditional_failure_result(
                            key, condition,
                            retrieve_value=NEVER_RETRIEVE,
                            return_existing_value=False)
                    raise
            else:
                if not self.discard(key):
                    return self._result_item_not_available(condition, satisfied)
                return self._result_delete_success(condition, actual_etag)

        # Attempt conditional write when possible
        if condition in (ETAG_IS_THE_SAME, ETAG_HAS_CHANGED):
            if_match, if_none_match = self._compute_conditional_headers(
                condition, expected_etag, actual_etag)
            try:
                resulting_etag = self._put_object_with_conditions(
                    key, value,
                    if_match=if_match, if_none_match=if_none_match)
                return self._result_write_success(
                    condition, actual_etag, resulting_etag, value)
            except ClientError as e:
                if conditional_request_failed(e):
                    return self._conditional_failure_result(
                        key, condition,
                        expected_etag=expected_etag,
                        retrieve_value=retrieve_value,
                        return_existing_value=True)
                raise

        # For ANY_ETAG: unconditional write
        resulting_etag = self._put_object_get_etag(key, value)
        return self._result_write_success(
            condition, actual_etag, resulting_etag, value)

    def _probe_conditional_delete(self) -> None:
        """Test whether the S3 backend enforces IfMatch on delete_object.

        Some S3-compatible backends (e.g. moto) silently ignore conditional
        headers on deletes. This probe runs once per class and caches the
        result so the fast path is only used when the backend supports it.
        """
        cls = type(self)
        if cls._conditional_delete_probed:
            return
        probe_key = self.root_prefix + "__persidict_probe__"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=probe_key, Body=b"probe")
            try:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name, Key=probe_key,
                    IfMatch='"__wrong_etag__"')
                # Delete succeeded with wrong IfMatch — backend doesn't enforce it
                cls._conditional_delete_supported = False
            except ClientError:
                # Backend correctly rejected the mismatched IfMatch
                cls._conditional_delete_supported = True
                self.s3_client.delete_object(
                    Bucket=self.bucket_name, Key=probe_key)
        except ClientError:
            cls._conditional_delete_supported = False
        cls._conditional_delete_probed = True

    def _probe_if_none_match_etag(self) -> None:
        """Test whether the S3 backend enforces IfNoneMatch with a specific ETag.

        Some S3-compatible backends (e.g. moto) only enforce
        IfNoneMatch: * (wildcard) but silently ignore IfNoneMatch with
        a concrete ETag value.  This probe runs once per class so the
        ETAG_HAS_CHANGED fast path is only used when the backend
        rejects a put whose current ETag matches the IfNoneMatch value.
        """
        cls = type(self)
        if cls._if_none_match_etag_probed:
            return
        probe_key = self.root_prefix + "__persidict_probe_inm__"
        try:
            resp = self.s3_client.put_object(
                Bucket=self.bucket_name, Key=probe_key, Body=b"probe")
            probe_etag = resp["ETag"]
            try:
                # IfNoneMatch with the object's own ETag — should be rejected
                self.s3_client.put_object(
                    Bucket=self.bucket_name, Key=probe_key,
                    Body=b"probe2", IfNoneMatch=probe_etag)
                # Write succeeded despite matching ETag — backend is broken
                cls._if_none_match_etag_supported = False
            except ClientError:
                # Backend correctly rejected the matching IfNoneMatch
                cls._if_none_match_etag_supported = True
            # Clean up probe object
            try:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name, Key=probe_key)
            except ClientError:
                pass
        except ClientError:
            cls._if_none_match_etag_supported = False
        cls._if_none_match_etag_probed = True

    def discard_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists
    ) -> ConditionalOperationResult[ValueType]:
        """Discard a key only if an ETag condition is satisfied.

        Uses S3 conditional delete with IfMatch to guard against
        concurrent changes for all condition types.

        Args:
            key: Dictionary key.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE.

        Returns:
            ConditionalOperationResult with the outcome.
        """
        key = NonEmptySafeStrTuple(key)

        # Fast path: ETAG_IS_THE_SAME with a real ETag — skip the HEAD.
        # Requires S3 backend to enforce IfMatch on delete_object.
        if (not self.append_only
                and condition is ETAG_IS_THE_SAME
                and not isinstance(expected_etag, ItemNotAvailableFlag)):
            if not type(self)._conditional_delete_probed:
                self._probe_conditional_delete()
            if type(self)._conditional_delete_supported:
                return self._discard_item_if_fast_path(
                    key, condition, expected_etag)

        return self._discard_item_if_fallback(
            key, condition, expected_etag)

    def _discard_item_if_fast_path(
            self,
            key: NonEmptySafeStrTuple,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists
    ) -> ConditionalOperationResult[ValueType]:
        """Optimistic S3 conditional delete for ETAG_IS_THE_SAME.

        Attempts a single S3 delete with IfMatch header,
        avoiding a separate ETag check round-trip.
        """
        obj_name = self._build_full_objectname(key)
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                IfMatch=expected_etag)
            return self._result_delete_success(condition, expected_etag)
        except ClientError as e:
            if not_found_error(e):
                return self._result_for_missing_key(condition, expected_etag)
            if not conditional_request_failed(e):
                raise
            return self._conditional_failure_result(
                key, condition,
                retrieve_value=NEVER_RETRIEVE,
                return_existing_value=False)

    def _discard_item_if_fallback(
            self,
            key: NonEmptySafeStrTuple,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists
    ) -> ConditionalOperationResult[ValueType]:
        """Check-then-delete path for conditions other than the fast path.

        Handles ETAG_HAS_CHANGED, ANY_ETAG, ETAG_IS_THE_SAME with
        ITEM_NOT_AVAILABLE, and append_only mode.
        """
        actual_etag = self._actual_etag(key)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if actual_etag is ITEM_NOT_AVAILABLE:
            return self._result_item_not_available(condition, satisfied)
        if not satisfied:
            return self._result_unchanged(
                condition, False, actual_etag, VALUE_NOT_RETRIEVED)
        if self.append_only:
            raise MutationPolicyError("append-only")

        # Atomic delete: guard against concurrent changes since the HEAD
        obj_name = self._build_full_objectname(key)
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                IfMatch=actual_etag)
            return self._result_delete_success(condition, actual_etag)
        except ClientError as e:
            if not_found_error(e) or conditional_request_failed(e):
                return self._conditional_failure_result(
                    key, condition,
                    retrieve_value=NEVER_RETRIEVE,
                    return_existing_value=False)
            raise

    def setdefault_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            default_value: ValueType,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Insert default_value if key is absent; conditioned on ETag check.

        Uses S3 conditional put (IfNoneMatch: ``*``) for atomic
        insert-if-absent when the key is absent and the condition is
        satisfied, avoiding the TOCTOU race in the base class.

        Args:
            key: Dictionary key.
            default_value: Value to insert if the key is absent and the
                condition is satisfied.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE
                if the caller believes the key is absent.
            retrieve_value: Controls value retrieval when the key exists.
                IF_ETAG_CHANGED (default) fetches only if
                expected_etag != actual_etag. ALWAYS_RETRIEVE fetches
                the existing value. NEVER_RETRIEVE returns
                VALUE_NOT_RETRIEVED.

        Returns:
            ConditionalOperationResult with the outcome of the operation.
        """
        if isinstance(default_value, Joker):
            raise TypeError("default_value must be a regular value, not a Joker command")
        self._validate_retrieve_value(retrieve_value)
        key = NonEmptySafeStrTuple(key)
        self._validate_value(default_value)

        actual_etag = self._actual_etag(key)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if actual_etag is not ITEM_NOT_AVAILABLE:
            if retrieve_value is NEVER_RETRIEVE or (
                    retrieve_value is IF_ETAG_CHANGED
                    and expected_etag == actual_etag):
                existing_value = VALUE_NOT_RETRIEVED
            else:
                existing_value = self[key]
            return self._result_unchanged(
                condition, satisfied, actual_etag, existing_value)

        if not satisfied:
            return self._result_item_not_available(condition, False)

        # Key is absent and condition is satisfied — atomic insert
        try:
            resulting_etag = self._put_object_with_conditions(
                key, default_value, if_none_match="*")
            return self._result_write_success(
                condition, ITEM_NOT_AVAILABLE, resulting_etag, default_value)
        except ClientError as e:
            if conditional_request_failed(e):
                # Concurrent writer inserted the key between our check
                # and our put — treat as "key already exists"
                actual_etag = self._actual_etag(key)
                if actual_etag is ITEM_NOT_AVAILABLE:
                    return self._result_item_not_available(condition, satisfied)
                if retrieve_value is NEVER_RETRIEVE:
                    existing_value = VALUE_NOT_RETRIEVED
                else:
                    # ALWAYS_RETRIEVE and IF_ETAG_CHANGED both fetch here:
                    # the key went from absent to present, so the ETag
                    # definitionally changed.
                    try:
                        existing_value = self[key]
                    except KeyError:
                        # Key deleted between etag check and read —
                        # report as absent.
                        return self._result_item_not_available(
                            condition, satisfied)
                return self._result_unchanged(
                    condition, satisfied, actual_etag, existing_value)
            raise

    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType | Joker) -> None:
        """Store a value for a key directly in S3.

        Handles special joker values (KEEP_CURRENT, DELETE_CURRENT) for
        conditional operations. Validates value types against base_class_for_values
        if specified, then serializes and uploads directly to S3.

        When append_only is True, uses ``setdefault_if`` with S3 conditional
        headers (IfNoneMatch: ``*``) for atomic insert-if-absent, avoiding
        the TOCTOU race of a separate existence check followed by an
        unconditional write.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptyPersiDictKey).
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Raises:
            MutationPolicyError: If attempting to modify an existing item
                when append_only is True.
            TypeError: If value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return

        if self.append_only:
            result = self.setdefault_if(
                key,
                default_value=value,
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=NEVER_RETRIEVE,
            )
            if not result.value_was_mutated:
                raise MutationPolicyError("append-only")
            return

        self._put_object_get_etag(key, value)


    def __delitem__(self, key: NonEmptyPersiDictKey):
        """Delete the stored value for a key from S3.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptyPersiDictKey).

        Raises:
            MutationPolicyError: If append_only is True.
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        self._process_delitem_args(key)
        # _process_delitem_args already verified existence via __contains__
        # (HEAD request), so skip the duplicate HEAD in _remove_item and
        # call delete_object directly.
        self._delete_object_ignoring_not_found(key)

    def _delete_object_ignoring_not_found(
            self, key: NonEmptySafeStrTuple) -> None:
        """Issue an S3 DeleteObject, silently ignoring 404/NoSuchKey."""
        obj_name = self._build_full_objectname(key)
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj_name)
        except ClientError as e:
            if not_found_error(e):
                pass
            else:
                raise

    def _remove_item(self, key: NonEmptySafeStrTuple) -> None:
        """Remove the S3 object for *key*.

        S3 ``delete_object`` is idempotent (returns 204 even for
        missing keys), so a HEAD check is performed first to raise
        ``KeyError`` when the key is absent — matching the contract
        that ``discard`` relies on.

        Raises:
            KeyError: If the object does not exist.
        """
        obj_name = self._build_full_objectname(key)
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
        except ClientError as e:
            if not_found_error(e):
                raise KeyError(key) from e
            raise
        self._delete_object_ignoring_not_found(key)

    def __len__(self) -> int:
        """Return the number of key-value pairs in the dictionary.

        Warning:
            This operation can be very slow and expensive on large S3 buckets
            as it must paginate through all objects under the dictionary's prefix.
            Avoid using in performance-critical code.

        Returns:
            Number of stored items under this dictionary's root_prefix.
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
            A generator that yields:
                - SafeStrTuple if result_type == {"keys"}
                - Any if result_type == {"values"}  
                - tuple[SafeStrTuple, Any] if result_type == {"keys", "values"}
                - tuple including float timestamp if "timestamps" requested

        Raises:
            TypeError: If base_class_for_values is set and a yielded value
                does not match it.
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
                The parsed key components with digest signatures intact.

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

                    unsigned_key = unsign_safe_str_tuple(
                        obj_key, 0)

                    value_to_return = None
                    if "values" in result_type:
                        # The object can be deleted between listing and fetching.
                        # Skip such races instead of raising to make iteration robust.
                        try:
                            value_to_return = self[unsigned_key]
                        except KeyError:
                            continue

                    timestamp_to_return = None
                    if "timestamps" in result_type:
                        timestamp_to_return = key["LastModified"].timestamp()

                    yield self._assemble_iter_result(
                        result_type
                        , key=unsigned_key
                        , value=value_to_return
                        , timestamp=timestamp_to_return)

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
            A new BasicS3Dict instance with root_prefix
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
            POSIX timestamp (seconds since Unix epoch) of the last
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
                raise KeyError(key) from e
            else:
                raise
