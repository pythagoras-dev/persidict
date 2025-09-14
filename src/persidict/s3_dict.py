from __future__ import annotations

import os
import tempfile
from typing import Any, Optional

import boto3
import joblib
import jsonpickle
from botocore.exceptions import ClientError

import parameterizable
from parameterizable.dict_sorter import sort_dict_by_keys

from .safe_str_tuple import SafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict
from .jokers import KEEP_CURRENT, DELETE_CURRENT, Joker
from .file_dir_dict import FileDirDict, PersiDictKey, non_empty_persidict_key
from .overlapping_multi_dict import OverlappingMultiDict

S3DICT_DEFAULT_BASE_DIR = "__s3_dict__"

class S3Dict(PersiDict):
    """A persistent dictionary that stores key-value pairs as S3 objects.
    
    Each key-value pair is stored as a separate S3 object in the specified bucket.
    
    A key can be either a string (object name without file extension) or a sequence
    of strings representing a hierarchical path (folder structure ending with an
    object name). Values can be instances of any Python type and are serialized
    to S3 objects.
    
    S3Dict supports multiple serialization formats:
    - Binary storage using pickle ('pkl' format)  
    - Human-readable text using jsonpickle ('json' format)
    - Plain text for string values (other formats)
    
    Note:
        Unlike native Python dictionaries, insertion order is not preserved.
        Operations may incur S3 API costs and network latency.
    """
    region: str
    bucket_name: str
    root_prefix: str
    file_type: str
    _base_dir: str

    def __init__(self, bucket_name: str = "my_bucket",
                 region: str = None,
                 root_prefix: str = "",
                 base_dir: str = S3DICT_DEFAULT_BASE_DIR,
                 file_type: str = "pkl",
                 immutable_items: bool = False,
                 digest_len: int = 8,
                 base_class_for_values: Optional[type] = None,
                 *args, **kwargs):
        """Initialize an S3-backed persistent dictionary.

        Args:
            bucket_name: Name of the S3 bucket to use. The bucket will be
                created automatically if it does not exist and permissions allow.
            region: AWS region for the bucket. If None, uses the default
                client region from AWS configuration.
            root_prefix: Common S3 key prefix under which all objects are
                stored. A trailing slash is automatically added if missing.
            base_dir: Local directory path used for temporary files and
                local caching of S3 objects.
            file_type: File extension/format for stored values. Supported formats:
                'pkl' (pickle), 'json' (jsonpickle), or custom text formats.
            immutable_items: If True, prevents modification of existing items
                after they are initially stored.
            digest_len: Number of base32 MD5 hash characters appended to key
                elements to prevent case-insensitive filename collisions. 
                Set to 0 to disable collision prevention.
            base_class_for_values: Optional base class that all stored values
                must inherit from. When specified (and not str), file_type
                must be 'pkl' or 'json' for proper serialization.
            *args: Additional positional arguments (ignored, reserved for compatibility).
            **kwargs: Additional keyword arguments (ignored, reserved for compatibility).
            
        Note:
            The S3 bucket will be created if it doesn't exist and AWS permissions
            allow. Network connectivity and valid AWS credentials are required.
        """

        super().__init__(immutable_items = immutable_items
                         , digest_len = digest_len
                         , base_class_for_values=base_class_for_values)
        self.file_type = file_type
        self.etag_file_type = f"{file_type}_etag"

        self.local_cache = OverlappingMultiDict(
            dict_type=FileDirDict,
            shared_subdicts_params={
                "base_dir": base_dir,
                "immutable_items": immutable_items,
                "base_class_for_values": base_class_for_values,
                "digest_len": digest_len
            },
            **{
                self.file_type: {},
                self.etag_file_type: {"base_class_for_values": str}
            }
        )

        self.main_cache = getattr(self.local_cache, self.file_type)
        self.etag_cache = getattr(self.local_cache, self.etag_file_type)

        self.region = region
        if region is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = boto3.client('s3', region_name=region)

        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NotFound':
                # Bucket does not exist, attempt to create it
                try:
                    self.s3_client.create_bucket(Bucket=bucket_name)
                except ClientError as create_e:
                    create_error_code = create_e.response['Error']['Code']
                    # Handle race condition where bucket was created by another process
                    # or the bucket name is already taken by another AWS account
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

        self.root_prefix=root_prefix
        if len(self.root_prefix) and self.root_prefix[-1] != "/":
            self.root_prefix += "/"


    def get_params(self):
        """Return configuration parameters as a dictionary.

        This method supports the Parameterizable API and is not part of
        the standard Python dictionary interface.

        Returns:
            dict: A mapping of parameter names to their configured values,
            including S3-specific parameters (region, bucket_name, root_prefix)
            combined with parameters from the local cache, sorted by key names.
        """
        params = self.main_cache.get_params()
        params["region"] = self.region
        params["bucket_name"] = self.bucket_name
        params["root_prefix"] = self.root_prefix
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    def base_url(self):
        """Return the S3 URL prefix of this dictionary.

        This property is not part of the standard Python dictionary interface.

        Returns:
            str: The base S3 URL in the format "s3://<bucket>/<root_prefix>".
        """
        return f"s3://{self.bucket_name}/{self.root_prefix}"


    @property
    def base_dir(self) -> str:
        """Return the dictionary's base directory in the local filesystem.

        This property is not part of the standard Python dictionary interface.

        Returns:
            str: Path to the local cache directory used for temporary files
            and caching S3 objects.
        """
        return self.main_cache.base_dir


    def _build_full_objectname(self, key: PersiDictKey) -> str:
        """Convert a key into a full S3 object key.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.

        Returns:
            str: The complete S3 object key including root_prefix and file_type
            extension, with digest-based collision prevention applied if enabled.
        """
        key = non_empty_persidict_key(key)
        key = sign_safe_str_tuple(key, self.digest_len)
        objectname = self.root_prefix +  "/".join(key)+ "." + self.file_type
        return objectname


    def __contains__(self, key: PersiDictKey) -> bool:
        """Check if the specified key exists in the dictionary.

        For immutable dictionaries, checks the local cache first. Otherwise,
        performs a HEAD request to S3 to verify object existence.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if the key exists in S3 (or local cache for immutable
            items), False otherwise.
        """
        key = non_empty_persidict_key(key)
        if self.immutable_items and key in self.main_cache:
                return True
        try:
            obj_name = self._build_full_objectname(key)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return True
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                self.main_cache.delete_if_exists(key)
                self.etag_cache.delete_if_exists(key)
                return False
            else:
                raise


    def __getitem__(self, key: PersiDictKey) -> Any:
        """Retrieve the value stored for a key.

        For immutable dictionaries with cached values, returns the cached copy.
        Otherwise, fetches from S3 using conditional requests (ETags) when
        available to minimize unnecessary downloads.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.

        Returns:
            Any: The deserialized value stored for the key.

        Raises:
            KeyError: If the key does not exist in S3.
        """

        key = non_empty_persidict_key(key)

        if self.immutable_items and key in self.main_cache:
            return self.main_cache[key]

        obj_name = self._build_full_objectname(key)

        cached_etag = None
        if not self.immutable_items and key in self.main_cache and key in self.etag_cache:
            cached_etag = self.etag_cache[key]

        try:
            get_kwargs = {'Bucket': self.bucket_name, 'Key': obj_name}
            if cached_etag:
                get_kwargs['IfNoneMatch'] = cached_etag

            response = self.s3_client.get_object(**get_kwargs)

            # 200 OK: object was downloaded, either because it's new or changed.
            s3_etag = response.get("ETag")
            body = response['Body']

            # Deserialize and cache the S3 object content
            if self.file_type == 'json':
                deserialized_value = jsonpickle.loads(body.read().decode('utf-8'))
            elif self.file_type == 'pkl':
                deserialized_value = joblib.load(body)
            else:
                deserialized_value = body.read().decode('utf-8')

            self.main_cache[key] = deserialized_value
            self.etag_cache[key] = s3_etag

        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 304:
                # HTTP 304 Not Modified: cached version is current, no download needed
                pass
            elif e.response.get("Error", {}).get("Code") == 'NoSuchKey':
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            else:
                # Re-raise other client errors (permissions, throttling, etc.)
                raise

        return self.main_cache[key]


    def __setitem__(self, key: PersiDictKey, value: Any):
        """Store a value for a key in both S3 and local cache.

        Handles special joker values (KEEP_CURRENT, DELETE_CURRENT) for
        conditional operations. Validates value types against base_class_for_values
        if specified, then stores locally and uploads to S3. Attempts to cache
        the S3 ETag for efficient future retrievals.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.
            value: Value to store, or a joker command (KEEP_CURRENT or 
                DELETE_CURRENT from the jokers module).

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """

        key = non_empty_persidict_key(key)
        PersiDict.__setitem__(self, key, value)
        if isinstance(value, Joker):
            # Joker values (KEEP_CURRENT, DELETE_CURRENT) are handled by base class
            return

        obj_name = self._build_full_objectname(key)

        # Store in local cache first
        self.main_cache[key] = value
        
        # Upload the serialized file from local cache to S3
        file_path = self.main_cache._build_full_path(key)
        self.s3_client.upload_file(file_path, self.bucket_name, obj_name)

        try:
            # Cache the S3 ETag for efficient conditional requests on future reads
            head = self.s3_client.head_object(
                Bucket=self.bucket_name, Key=obj_name)
            self.etag_cache[key] = head.get("ETag")
        except ClientError:
            # Remove stale ETag on failure to force fresh downloads later
            self.etag_cache.delete_if_exists(key)


    def __delitem__(self, key: PersiDictKey):
        """Delete the stored value for a key from both S3 and local cache.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If immutable_items is True, or if the key does not exist.
        """
        key = non_empty_persidict_key(key)
        PersiDict.__delitem__(self, key)
        obj_name = self._build_full_objectname(key)
        self.s3_client.delete_object(Bucket = self.bucket_name, Key = obj_name)
        self.etag_cache.delete_if_exists(key)
        self.main_cache.delete_if_exists(key)


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
        suffix = "." + self.file_type

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name, Prefix = self.root_prefix)

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

        PersiDict._generic_iter(self, result_type)

        suffix = "." + self.file_type
        ext_len = len(self.file_type) + 1
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
                Bucket=self.bucket_name, Prefix = self.root_prefix)

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

                    if "keys" in result_type:
                        key_to_return = unsign_safe_str_tuple(
                            obj_key, self.digest_len)
                        to_return.append(key_to_return)

                    if "values" in result_type:
                        value_to_return = self[obj_key]
                        to_return.append(value_to_return)

                    if len(result_type) == 1:
                        yield to_return[0]
                    else:
                        if "timestamps" in result_type:
                            timestamp_to_return = key["LastModified"].timestamp()
                            to_return.append(timestamp_to_return)
                        yield tuple(to_return)

        return step()


    def get_subdict(self, key: PersiDictKey) -> S3Dict:
        """Create a subdictionary scoped to items with the specified prefix.

        Returns an empty subdictionary if no items exist under the prefix.
        This method is not part of the standard Python dictionary interface.

        Args:
            key (PersiDictKey): A common prefix (string or sequence of strings)
                used to scope items stored under this dictionary.

        Returns:
            S3Dict: A new S3Dict instance with root_prefix extended by the given
            key, sharing the parent's bucket, region, file_type, and other
            configuration settings.
        """

        key = SafeStrTuple(key)
        if len(key):
            key = sign_safe_str_tuple(key, self.digest_len)
            full_root_prefix = self.root_prefix +  "/".join(key)
        else:
            full_root_prefix = self.root_prefix

        new_dir_path = self.main_cache._build_full_path(
            key, create_subdirs = True, is_file_path = False)

        new_dict = S3Dict(
            bucket_name = self.bucket_name
            , region = self.region
            , root_prefix = full_root_prefix
            , base_dir = new_dir_path
            , file_type = self.file_type
            , immutable_items = self.immutable_items
            , digest_len = self.digest_len
            , base_class_for_values = self.base_class_for_values)

        return new_dict


    def timestamp(self, key: PersiDictKey) -> float:
        """Get the last modification timestamp for a key.

        This method is not part of the standard Python dictionary interface.

        Args:
            key: Dictionary key (string or sequence of strings) or SafeStrTuple.

        Returns:
            float: POSIX timestamp (seconds since Unix epoch) of the last
            modification time as reported by S3. The timestamp is timezone-aware
            and converted to UTC.

        Raises:
            KeyError: If the key does not exist in S3.
        """
        key = non_empty_persidict_key(key)
        obj_name = self._build_full_objectname(key)
        response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
        return response["LastModified"].timestamp()


parameterizable.register_parameterizable_class(S3Dict)