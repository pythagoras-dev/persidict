from __future__ import annotations

import os
import tempfile
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

import parameterizable
from parameterizable.dict_sorter import sort_dict_by_keys

from .safe_str_tuple import SafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict
from .jokers import KEEP_CURRENT, DELETE_CURRENT
from .file_dir_dict import FileDirDict, PersiDictKey

S3DICT_DEFAULT_BASE_DIR = "__s3_dict__"

class S3Dict(PersiDict):
    """ A persistent dictionary that stores key-value pairs as S3 objects.

    A new object is created for each key-value pair.

    A key is either an objectname (a 'filename' without an extension),
    or a sequence of folder names (object name prefixes) that ends
    with an objectname. A value can be an instance of any Python type,
    and will be stored as an S3-object.

    S3Dict can store objects in binary objects (as pickles)
    or in human-readable texts objects (using jsonpickles).

    Unlike in native Python dictionaries, insertion order is not preserved.
    """
    region: str
    bucket_name: str
    root_prefix: str
    file_type: str
    _base_dir: str

    def __init__(self, bucket_name:str = "my_bucket"
                 , region:str = None
                 , root_prefix:str = ""
                 , base_dir:str = S3DICT_DEFAULT_BASE_DIR
                 , file_type:str = "pkl"
                 , immutable_items:bool = False
                 , digest_len:int = 8
                 , base_class_for_values:Optional[type] = None
                 ,*args ,**kwargs):
        """Initialize an S3-backed persistent dictionary.

        Args:
            bucket_name (str): Name of the S3 bucket to use. The bucket will be
                created if it does not already exist.
            region (str | None): AWS region of the bucket. If None, the default
                client region is used.
            root_prefix (str): Common S3 key prefix under which all objects are
                stored. A trailing slash is added if missing.
            base_dir (str): Local directory used for temporary files and a
                small on-disk cache.
            file_type (str): Extension/format for stored values. "pkl" or
                "json" store arbitrary Python objects; other values imply plain
                text and only allow str values.
            immutable_items (bool): If True, disallow changing existing items.
            digest_len (int): Number of base32 MD5 characters appended to key
                elements to avoid case-insensitive collisions. Use 0 to disable.
            base_class_for_values (type | None): Optional base class that all
                values must inherit from. If provided and not str, file_type
                must be "pkl" or "json".
            *args: Ignored; reserved for compatibility.
            **kwargs: Ignored; reserved for compatibility.

        Raises:
            ValueError: If file_type is "__etag__" (reserved) or configuration
                is inconsistent with base_class_for_values.
        """

        super().__init__(immutable_items = immutable_items, digest_len = 0)
        self.file_type = file_type
        if self.file_type == "__etag__":
            raise ValueError(
                "file_type cannot be 'etag' as it is a reserved extension for caching.")

        self.local_cache = FileDirDict(
            base_dir= base_dir
            , file_type = file_type
            , immutable_items = immutable_items
            , base_class_for_values=base_class_for_values
            , digest_len = digest_len)

        self.region = region
        if region is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = boto3.client('s3', region_name=region)

        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
        except:
            pass

        self.bucket_name = bucket_name

        self.root_prefix=root_prefix
        if len(self.root_prefix) and self.root_prefix[-1] != "/":
            self.root_prefix += "/"


    def get_params(self):
        """Return configuration parameters of the object as a dictionary.

        This method is needed to support Parameterizable API.
        The method is absent in the original dict API.

        Returns:
            dict: A mapping of parameter names to their configured values,
            including region, bucket_name, and root_prefix combined with
            parameters from the local cache.
        """
        params = self.local_cache.get_params()
        params["region"] = self.region
        params["bucket_name"] = self.bucket_name
        params["root_prefix"] = self.root_prefix
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    def base_url(self):
        """Return the S3 URL prefix of this dictionary.

        This property is absent in the original dict API.

        Returns:
            str: The base S3 URL in the form "s3://<bucket>/<root_prefix>".
        """
        return f"s3://{self.bucket_name}/{self.root_prefix}"


    @property
    def base_dir(self) -> str:
        """Return dictionary's base directory in the local filesystem.

        This property is absent in the original dict API.

        Returns:
            str: Path to the local on-disk cache directory used by S3Dict.
        """
        return self.local_cache.base_dir


    def _build_full_objectname(self, key:PersiDictKey) -> str:
        """Convert a key into a full S3 object key (object name).

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            str: The full S3 key under root_prefix with file_type suffix applied.
        """
        key = SafeStrTuple(key)
        key = sign_safe_str_tuple(key, self.digest_len)
        objectname = self.root_prefix +  "/".join(key)+ "." + self.file_type
        return objectname


    def __contains__(self, key:PersiDictKey) -> bool:
        """Return True if the specified key exists in S3.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if the object exists (or is cached when immutable), else False.
        """
        key = SafeStrTuple(key)
        if self.immutable_items:
            file_name = self.local_cache._build_full_path(
                key, create_subdirs=True)
            if os.path.exists(file_name):
                return True
        try:
            obj_name = self._build_full_objectname(key)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
            return True
        except:
            return False


    def _write_etag_file(self, file_name: str, etag: str):
        """Atomically write the ETag to its cache file.

        Args:
            file_name (str): Path to the cached data file (without the ETag suffix).
            etag (str): The S3 ETag value to persist alongside the cached file.
        """
        if not etag:
            return
        etag_file_name = file_name + ".__etag__"
        dir_name = os.path.dirname(etag_file_name)
        # Write to a temporary file and then rename for atomicity
        fd, temp_path = tempfile.mkstemp(dir=dir_name)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(etag)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, etag_file_name)
            try:
                if os.name == 'posix':
                    dir_fd = os.open(dir_name, os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
            except OSError:
                pass
        except:
            os.remove(temp_path)
            raise


    def __getitem__(self, key:PersiDictKey) -> Any:
        """Retrieve the value stored for a key from S3 or local cache.

        If immutable_items is True and a local cached file exists, that cache is
        returned. Otherwise, the object is fetched from S3, with conditional
        requests used when possible.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            Any: The stored value.
        """

        key = SafeStrTuple(key)
        file_name = self.local_cache._build_full_path(key, create_subdirs=True)

        if self.immutable_items and os.path.exists(file_name):
            return self.local_cache._read_from_file(file_name)

        obj_name = self._build_full_objectname(key)

        cached_etag = None
        etag_file_name = file_name + ".__etag__"
        if not self.immutable_items and os.path.exists(file_name) and os.path.exists(
                etag_file_name):
            with open(etag_file_name, "r") as f:
                cached_etag = f.read()

        try:
            get_kwargs = {'Bucket': self.bucket_name, 'Key': obj_name}
            if cached_etag:
                get_kwargs['IfNoneMatch'] = cached_etag

            response = self.s3_client.get_object(**get_kwargs)

            # 200 OK: object was downloaded, either because it's new or changed.
            s3_etag = response.get("ETag")
            body = response['Body']

            dir_name = os.path.dirname(file_name)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=".__tmp__")

            try:
                with os.fdopen(fd, 'wb') as f:
                    # Stream body to file to avoid loading all in memory
                    for chunk in body.iter_chunks():
                        f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, file_name)
                try:
                    if os.name == 'posix':
                        dir_fd = os.open(dir_name, os.O_RDONLY)
                        try:
                            os.fsync(dir_fd)
                        finally:
                            os.close(dir_fd)
                except OSError:
                    pass
            except:
                os.remove(temp_path)  # Clean up temp file on failure
                raise

            self._write_etag_file(file_name, s3_etag)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 304:
                # 304 Not Modified: our cached version is up-to-date.
                # The file will be read from cache at the end of the function.
                pass
            elif e.response.get("Error", {}).get("Code") == 'NoSuchKey':
                raise KeyError(f"Key {key} not found in S3 bucket {self.bucket_name}")
            else:
                # Re-raise other client errors (e.g., permissions, throttling)
                raise

        return self.local_cache._read_from_file(file_name)


    def __setitem__(self, key:PersiDictKey, value:Any):
        """Store a value for a key in S3 and update the local cache.

        Interprets joker values KEEP_CURRENT and DELETE_CURRENT accordingly.
        Validates a value type if base_class_for_values is set, then writes to the
        local cache and uploads to S3. If possible, caches the S3 ETag locally to
        enable conditional GETs later.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.
            value (Any): Value to store, or a joker command.

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If value is a PersiDict or does not match
                base_class_for_values when it is set.
        """

        if value is KEEP_CURRENT:
            return

        if value is DELETE_CURRENT:
            self.delete_if_exists(key)
            return

        if isinstance(value, PersiDict):
            raise TypeError(
                f"You are not allowed to store a PersiDict "
                + f"inside another PersiDict.")

        if self.base_class_for_values is not None:
            if not isinstance(value, self.base_class_for_values):
                raise TypeError(
                    f"Value must be of type {self.base_class_for_values},"
                    + f"but it is {type(value)} instead." )

        key = SafeStrTuple(key)

        if self.immutable_items and key in self:
            raise KeyError("Can't modify an immutable item")

        file_name = self.local_cache._build_full_path(key, create_subdirs=True)
        obj_name = self._build_full_objectname(key)

        self.local_cache._save_to_file(file_name, value)
        self.s3_client.upload_file(file_name, self.bucket_name, obj_name)

        try:
            head = self.s3_client.head_object(
                Bucket=self.bucket_name, Key=obj_name)
            s3_etag = head.get("ETag")
            self._write_etag_file(file_name, s3_etag)
        except ClientError:
            # If we can't get ETag, we should remove any existing etag file
            # to force a re-download on the next __getitem__ call.
            etag_file_name = file_name + ".__etag__"
            if os.path.exists(etag_file_name):
                os.remove(etag_file_name)


    def __delitem__(self, key:PersiDictKey):
        """Delete the stored value for a key from S3 and local cache.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If immutable_items is True.
        """

        key = SafeStrTuple(key)
        if self.immutable_items:
            raise KeyError("Can't delete an immutable item")
        obj_name = self._build_full_objectname(key)
        self.s3_client.delete_object(Bucket = self.bucket_name, Key = obj_name)
        file_name = self.local_cache._build_full_path(key)
        if os.path.isfile(file_name):
            os.remove(file_name)
        etag_file_name = file_name + ".__etag__"
        if os.path.isfile(etag_file_name):
            os.remove(etag_file_name)

    def __len__(self) -> int:
        """Return len(self).

        WARNING: This operation can be very slow and costly on large S3 buckets
        as it needs to iterate over all objects in the dictionary's prefix.
        Avoid using it in performance-sensitive code.

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


    def _generic_iter(self, result_type: str):
        """Underlying implementation for .items()/.keys()/.values() iterators.

        Iterates over S3 objects under the configured root_prefix and yields
        keys, values, and/or timestamps according to the requested result_type.
        Keys are mapped to SafeStrTuple by removing the file extension and
        unsigning based on digest_len.

        Args:
            result_type (set[str]): Any non-empty subset of {"keys", "values",
                "timestamps"} specifying which fields to yield.

        Returns:
            Iterator: A generator yielding:
                - SafeStrTuple if result_type == {"keys"}
                - Any if result_type == {"values"}
                - tuple[SafeStrTuple, Any] if result_type == {"keys", "values"}
                - tuple[..., float] including POSIX timestamp if "timestamps" is requested.

        Raises:
            ValueError: If result_type is not a set or contains entries other than
                "keys", "values", and/or "timestamps", or if it is empty.
        """

        if not isinstance(result_type, set):
            raise ValueError(
                "result_type must be a set containing one to three of: 'keys', 'values', 'timestamps'"
            )
        if not (1 <= len(result_type) <= 3):
            raise ValueError("result_type must be a non-empty set with at most three elements")
        allowed = {"keys", "values", "timestamps"}
        if not result_type.issubset(allowed):
            invalid = ", ".join(sorted(result_type - allowed))
            raise ValueError(f"result_type contains invalid entries: {invalid}. Allowed: {sorted(allowed)}")
        # Intersections/length checks are implied by the above conditions.

        suffix = "." + self.file_type
        ext_len = len(self.file_type) + 1
        prefix_len = len(self.root_prefix)

        def splitter(full_name: str) -> SafeStrTuple:
            """Convert an S3 object key into a SafeStrTuple without the suffix.

            Args:
                full_name (str): Full S3 object key (including root_prefix).

            Returns:
                SafeStrTuple: The parsed key parts, still signed.

            Raises:
                ValueError: If the provided key does not start with this dictionary's root_prefix.
            """
            if not full_name.startswith(self.root_prefix):
                raise ValueError(
                    f"S3 object key '{full_name}' is outside of root_prefix '{self.root_prefix}'"
                )
            result = full_name[prefix_len:-ext_len].split(sep="/")
            return SafeStrTuple(result)

        def step():
            """Generator that pages through S3 and yields entries based on result_type."""
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


    def get_subdict(self, key:PersiDictKey) -> S3Dict:
        """Get a subdictionary containing items with the same prefix key.

        For a non-existing prefix key, an empty sub-dictionary is returned.
        This method is absent in the original dict API.

        Args:
            key (PersiDictKey): A common prefix (string or sequence of strings)
                used to scope items stored under this dictionary.

        Returns:
            S3Dict: A new S3Dict instance rooted at the given prefix, sharing
            the same bucket, region, serialization, and immutability settings.
        """

        key = SafeStrTuple(key)
        if len(key):
            key = SafeStrTuple(key)
            key = sign_safe_str_tuple(key, self.digest_len)
            full_root_prefix = self.root_prefix +  "/".join(key)
        else:
            full_root_prefix = self.root_prefix

        new_dir_path = self.local_cache._build_full_path(
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


    def timestamp(self,key:PersiDictKey) -> float:
        """Get last modification time (Unix epoch seconds) for a key.

        This method is absent in the original dict API.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            float: POSIX timestamp (seconds since the Unix epoch) of the last
            modification time as reported by S3 for the object.
        """
        # TODO: check work with timezones
        key = SafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
        return response["LastModified"].timestamp()


parameterizable.register_parameterizable_class(S3Dict)