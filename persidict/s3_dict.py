from __future__ import annotations

import os
from typing import Any

import boto3

from .safe_str_tuple import SafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict
from .file_dir_dict import FileDirDict, PersiDictKey


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


    def __init__(self, bucket_name:str
                 , region:str = None
                 , root_prefix:str = ""
                 , dir_name:str = "S3_Dict"
                 , file_type:str = "pkl"
                 , immutable_items:bool = False
                 , digest_len:int = 8
                 ,*args ,**kwargs):
        """A constructor defines location of the store and object format to use.

        bucket_name and region define an S3 location of the storage
        that will contain all the objects in the S3_Dict.
        If the bucket does not exist, it will be created.

        root_prefix is a common S3 prefix for all objectnames in a dictionary.

        dir_name is a local directory that will be used to store tmp files.

        file_type can take one of two values: "pkl" or "json".
        It defines which object format will be used by S3_Dict
        to store values.
        """

        super().__init__(immutable_items = immutable_items, digest_len = 0)
        self.file_type = file_type

        self.local_cache = FileDirDict(
            dir_name = dir_name
            , file_type = file_type
            , immutable_items = immutable_items
            , digest_len = digest_len)

        self.region = region
        if region is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = boto3.client('s3', region_name=region)

        self.bucket = self.s3_client.create_bucket(Bucket=bucket_name)
        self.bucket_name = bucket_name

        self.root_prefix=root_prefix
        if len(self.root_prefix) and self.root_prefix[-1] != "/":
            self.root_prefix += "/"


    def __repr__(self):
        """Return repr(self)."""

        repr_str = super().__repr__()
        repr_str = repr_str[:-1] + f", dir_name={self.local_cache.base_dir}"
        repr_str += f", file_type={self.file_type}"
        repr_str += f", region={self.region}"
        repr_str += f", bucket_name={self.bucket_name}"
        repr_str += f", root_prefix={self.root_prefix}"
        repr_str += " )"

        return repr_str


    def _build_full_objectname(self, key:PersiDictKey) -> str:
        """ Convert PersiDictKey into an S3 objectname. """
        key = SafeStrTuple(key)
        key = sign_safe_str_tuple(key, self.digest_len)
        objectname = self.root_prefix +  "/".join(key)+ "." + self.file_type
        return objectname


    def __contains__(self, key:PersiDictKey) -> bool:
        """True if the dictionary has the specified key, else False. """
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


    def __getitem__(self, key:PersiDictKey) -> Any:
        """X.__getitem__(y) is an equivalent to X[y]. """

        key = SafeStrTuple(key)
        file_name = self.local_cache._build_full_path(key, create_subdirs=True)

        if self.immutable_items:
            try:
                result = self.local_cache._read_from_file(file_name)
                return result
            except:
                pass

        obj_name = self._build_full_objectname(key)
        self.s3_client.download_file(self.bucket_name, obj_name, file_name)
        result = self.local_cache._read_from_file(file_name)
        if not self.immutable_items:
            os.remove(file_name)

        return result


    def __setitem__(self, key:PersiDictKey, value:Any):
        """Set self[key] to value. """

        key = SafeStrTuple(key)
        file_name = self.local_cache._build_full_path(key, create_subdirs=True)
        obj_name = self._build_full_objectname(key)

        if self.immutable_items:
            key_is_present = False
            if os.path.exists(file_name):
                key_is_present = True
            else:
                try:
                    self.s3_client.head_object(
                        Bucket=self.bucket_name, Key=obj_name)
                    key_is_present = True
                except:
                    key_is_present = False

            assert not key_is_present, "Can't modify an immutable item"

        self.local_cache._save_to_file(file_name, value)
        self.s3_client.upload_file(file_name, self.bucket_name, obj_name)
        if not self.immutable_items:
            os.remove(file_name)


    def __delitem__(self, key:PersiDictKey):
        """Delete self[key]. """

        key = SafeStrTuple(key)
        assert not self.immutable_items, "Can't delete an immutable item"
        obj_name = self._build_full_objectname(key)
        self.s3_client.delete_object(Bucket = self.bucket_name, Key = obj_name)
        file_name = self.local_cache._build_full_path(key)
        if os.path.isfile(file_name):
            os.remove(file_name)


    def __len__(self) -> int:
        """Return len(self). """

        num_files = 0
        suffix = "." + self.file_type

        paginator = self.s3_client.get_paginator("list_objects")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name, Prefix = self.root_prefix)

        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    obj_name = key["Key"]
                    if obj_name.endswith(suffix):
                        num_files += 1

        return num_files


    def _generic_iter(self, iter_type: str):
        """Underlying implementation for .items()/.keys()/.values() iterators"""
        assert iter_type in {"keys", "values", "items"}
        suffix = "." + self.file_type
        ext_len = len(self.file_type) + 1
        prefix_len = len(self.root_prefix)

        def splitter(full_name: str) -> SafeStrTuple:
            assert full_name.startswith(self.root_prefix)
            result = full_name[prefix_len:-ext_len].split(sep="/")
            return SafeStrTuple(result)

        def step():
            paginator = self.s3_client.get_paginator("list_objects")
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name, Prefix = self.root_prefix)

            for page in page_iterator:
                if "Contents" in page:
                    for key in page["Contents"]:
                        obj_name = key["Key"]
                        if not obj_name.endswith(suffix):
                            continue
                        obj_key = splitter(obj_name)
                        if iter_type == "keys":
                            yield unsign_safe_str_tuple(
                                obj_key, self.digest_len)
                        elif iter_type == "values":
                            yield self[obj_key]
                        else:
                            yield (unsign_safe_str_tuple(
                                obj_key, self.digest_len), self[obj_key])

        return step()


    def get_subdict(self, key:PersiDictKey) -> S3Dict:
        """Get a subdictionary containing items with the same prefix_key.

        This method is absent in the original dict API.
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

        return S3Dict(
            bucket_name = self.bucket_name
            , region = self.region
            , root_prefix = full_root_prefix
            , dir_name = new_dir_path
            , file_type = self.file_type
            , immutable_items = self.immutable_items)


    def mtimestamp(self,key:PersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.
        """
        #TODO: check work with timezones
        key = SafeStrTuple(key)
        obj_name = self._build_full_objectname(key)
        response = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj_name)
        return response["LastModified"].timestamp()