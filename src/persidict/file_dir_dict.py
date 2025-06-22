""" Persistent dictionaries that store key-value pairs on local disks.

This functionality is implemented by the class FileDirDict
(inherited from PersiDict): a dictionary that
stores key-value pairs as files on a local hard-drive.
A key is used to compose a filename, while a value is stored in the file
as a binary, or as a json object, or as a plain text
(depends on configuration parameters).
"""
from __future__ import annotations

import os
import random
import time
from typing import Any, Optional

import joblib
import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
import parameterizable
from parameterizable import sort_dict_by_keys

from .jokers import KEEP_CURRENT, DELETE_CURRENT, Joker
from .safe_chars import replace_unsafe_chars
from .safe_str_tuple import SafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict, PersiDictKey


jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()

FILEDIRDICT_DEFAULT_BASE_DIR = "__file_dir_dict__"

class FileDirDict(PersiDict):
    """ A persistent Dict that stores key-value pairs in local files.

    A new file is created for each key-value pair.
    A key is either a filename (without an extension),
    or a sequence of directory names that ends with a filename.
    A value can be any Python object, which is stored in a file.
    Insertion order is not preserved.

    FileDirDict can store objects in binary files or in human-readable
    text files (either in jason format or as a plain text).
    """

    _base_dir:str
    file_type:str

    def __init__(self
                 , base_dir: str = FILEDIRDICT_DEFAULT_BASE_DIR
                 , file_type: str = "pkl"
                 , immutable_items:bool = False
                 , digest_len:int = 8
                 , base_class_for_values: Optional[type] = None):
        """A constructor defines location of the store and file format to use.

        _base_dir is a directory that will contain all the files in
        the FileDirDict. If the directory does not exist, it will be created.

        base_class_for_values constraints the type of values that can be
        stored in the dictionary. If specified, it will be used to
        check types of values in the dictionary. If not specified,
        no type checking will be performed and all types will be allowed.

        file_type is extension, which will be used for all files in the dictionary.
        If file_type has one of two values: "pkl" or "json", it defines
        which file format will be used by FileDirDict to store values.
        For all other values of file_type, the file format will always be plain
        text. "pkl" and "json" allow to store arbitrary Python objects,
        while all other file_type-s only work with str objects.
        """

        super().__init__(immutable_items = immutable_items
                ,digest_len = digest_len
                ,base_class_for_values = base_class_for_values)

        assert file_type == replace_unsafe_chars(file_type, "")
        self.file_type = file_type

        if (base_class_for_values is None or
                not issubclass(base_class_for_values,str)):
            assert file_type in {"json", "pkl"}, ("For non-string values"
                + " file_type must be either 'pkl' or 'json'.")

        base_dir = str(base_dir)

        if os.path.isfile(base_dir):
            raise ValueError(f"{base_dir} is a file, not a directory.")

        try: # extra protection to better handle concurrent access
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)
        except:
            time.sleep(random.random()/random.randint(1, 3))
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)
        assert os.path.isdir(base_dir)

        # self.base_dir_param = _base_dir
        self._base_dir = os.path.abspath(base_dir)


    def __repr__(self):
        """Return repr(self)."""

        repr_str = super().__repr__()
        repr_str = repr_str[:-1] + f", _base_dir={self._base_dir}"
        repr_str += f", file_type={self.file_type}"
        repr_str += " )"

        return repr_str


    def get_params(self):
        """Return configuration parameters of the dictionary.

        This method is needed to support Parameterizable API.
        The method is absent in the original dict API.
        """
        params = PersiDict.get_params(self)
        additional_params = dict(
            base_dir=self.base_dir
            , file_type=self.file_type)
        params.update(additional_params)
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    def base_url(self) -> str:
        """Return dictionary's URL.

        This property is absent in the original dict API.
        """
        return f"file://{self._base_dir}"


    @property
    def base_dir(self) -> str:
        """Return dictionary's base directory.

        This property is absent in the original dict API.
        """
        return self._base_dir


    def __len__(self) -> int:
        """ Get the number of key-value pairs in the dictionary."""

        num_files = 0
        suffix = "." + self.file_type
        for subdir_info in os.walk(self._base_dir):
            files = subdir_info[2]
            files = [f_name for f_name in files
                     if f_name.endswith(suffix)]
            num_files += len(files)
        return num_files


    def clear(self) -> None:
        """ Remove all elements from the dictionary."""

        if self.immutable_items:
            raise KeyError("Can't clear a dict that contains immutable items")

        for subdir_info in os.walk(self._base_dir, topdown=False):
            (subdir_name, _, files) = subdir_info
            suffix = "." + self.file_type
            for f in files:
                if f.endswith(suffix):
                    os.remove(os.path.join(subdir_name, f))
            if (subdir_name != self._base_dir) and (
                    len(os.listdir(subdir_name)) == 0 ):
                os.rmdir(subdir_name)

    def _build_full_path(self
                         , key:SafeStrTuple
                         , create_subdirs:bool=False
                         , is_file_path:bool=True) -> str:
        """Convert a key into a filesystem path."""

        key = sign_safe_str_tuple(key, self.digest_len)
        key = [self._base_dir] + list(key.strings)
        dir_names = key[:-1] if is_file_path else key

        if create_subdirs:
            current_dir = dir_names[0]
            for dir_name in dir_names[1:]:
                new_dir = os.path.join(current_dir, dir_name)
                try: # extra protection to better handle concurrent access
                    if not os.path.isdir(new_dir):
                        os.mkdir(new_dir)
                except:
                    time.sleep(random.random()/random.randint(1, 3))
                    if not os.path.isdir(new_dir):
                        os.mkdir(new_dir)
                current_dir = new_dir

        if is_file_path:
            file_name = key[-1] + "." + self.file_type
            return os.path.join(*dir_names, file_name)
        else:
            return os.path.join(*dir_names)


    def get_subdict(self, key:PersiDictKey) -> FileDirDict:
        """Get a subdictionary containing items with the same prefix key.

        For non-existing prefix key, an empty sub-dictionary is returned.

        This method is absent in the original dict API.
        """
        key = SafeStrTuple(key)
        full_dir_path = self._build_full_path(
            key, create_subdirs = True, is_file_path = False)
        return FileDirDict(
            base_dir= full_dir_path
            , file_type=self.file_type
            , immutable_items= self.immutable_items
            , digest_len=self.digest_len
            , base_class_for_values=self.base_class_for_values)


    def _read_from_file_impl(self, file_name:str) -> Any:
        """Read a value from a file. """

        if self.file_type == "pkl":
            with open(file_name, 'rb') as f:
                result = joblib.load(f)
        elif self.file_type == "json":
            with open(file_name, 'r') as f:
                result = jsonpickle.loads(f.read())
        else:
            with open(file_name, 'r') as f:
                result = f.read()
        return result


    def _read_from_file(self,file_name:str) -> Any:
        """Read a value from a file. """

        if not (self.file_type in {"pkl", "json"} or issubclass(
            self.base_class_for_values, str)):
            raise ValueError("When base_class_for_values is not str,"
                + " file_type must be pkl or json.")

        n_retries = 8
        # extra protections to better handle concurrent writes
        for i in range(n_retries):
            try:
                return self._read_from_file_impl(file_name)
            except:
                time.sleep(random.random()/random.randint(1, 10))

        return self._read_from_file_impl(file_name)


    def _save_to_file_impl(self, file_name:str, value:Any) -> None:
        """Save a value to a file. """

        if self.file_type == "pkl":
            with open(file_name, 'wb') as f:
                joblib.dump(value, f, compress='lz4')
        elif self.file_type == "json":
            with open(file_name, 'w') as f:
                f.write(jsonpickle.dumps(value, indent=4))
        else:
            with open(file_name, 'w') as f:
                f.write(value)


    def _save_to_file(self, file_name:str, value:Any) -> None:
        """Save a value to a file. """

        if not (self.file_type in {"pkl", "json"} or issubclass(
            self.base_class_for_values, str)):
            raise ValueError("When base_class_for_values is not str,"
                + " file_type must be pkl or json.")

        n_retries = 3
        # extra protections to better handle concurrent writes
        for i in range(n_retries):
            try: # extra protections to better handle concurrent writes
                self._save_to_file_impl(file_name, value)
                return
            except:
                time.sleep(random.random()/random.randint(1, 5))

        self._save_to_file_impl(file_name, value)


    def __contains__(self, key:PersiDictKey) -> bool:
        """True if the dictionary has the specified key, else False. """
        key = SafeStrTuple(key)
        filename = self._build_full_path(key)
        return os.path.isfile(filename)


    def __getitem__(self, key:PersiDictKey) -> Any:
        """ Implementation for x[y] syntax. """
        key = SafeStrTuple(key)
        filename = self._build_full_path(key)
        if not os.path.isfile(filename):
            raise KeyError(f"File {filename} does not exist")
        result = self._read_from_file(filename)
        if self.base_class_for_values is not None:
            if not isinstance(result, self.base_class_for_values):
                raise TypeError(
                    f"Value must be of type {self.base_class_for_values},"
                    + f" but it is {type(result)} instead.")
        return result


    def __setitem__(self, key:PersiDictKey, value:Any):
        """Set self[key] to value."""

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
                    + f"but it is {type(value)} instead.")

        key = SafeStrTuple(key)
        filename = self._build_full_path(key, create_subdirs=True)
        if self.immutable_items and os.path.exists(filename):
            raise KeyError("Can't modify an immutable item")
        self._save_to_file(filename, value)


    def __delitem__(self, key:PersiDictKey) -> None:
        """Delete self[key]."""
        key = SafeStrTuple(key)
        assert not self.immutable_items, "Can't delete immutable items"
        filename = self._build_full_path(key)
        if not os.path.isfile(filename):
            raise KeyError(f"File {filename} does not exist")
        os.remove(filename)


    def _generic_iter(self, iter_type: str):
        """Underlying implementation for .items()/.keys()/.values() iterators"""
        assert iter_type in {"keys", "values", "items"}
        walk_results = os.walk(self._base_dir)
        ext_len = len(self.file_type) + 1

        def splitter(dir_path: str):
            """Transform a dirname into a PersiDictKey key"""
            result = []
            if dir_path == ".":
                return result
            while True:
                head, tail = os.path.split(dir_path)
                result = [tail] + result
                dir_path = head
                if len(head) == 0:
                    break
            return tuple(result)

        def step():
            suffix = "." + self.file_type
            for dir_name, _, files in walk_results:
                for f in files:
                    if f.endswith(suffix):
                        prefix_key = os.path.relpath(
                            dir_name, start=self._base_dir)

                        result_key = (*splitter(prefix_key), f[:-ext_len])
                        result_key = SafeStrTuple(result_key)

                        if iter_type == "keys":
                            yield unsign_safe_str_tuple(
                                result_key, self.digest_len)
                        elif iter_type == "values":
                            yield self[result_key]
                        else:
                            yield (unsign_safe_str_tuple(
                                result_key, self.digest_len), self[result_key])

        return step()


    def timestamp(self, key:PersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.
        """
        key = SafeStrTuple(key)
        filename = self._build_full_path(key)
        return os.path.getmtime(filename)


parameterizable.register_parameterizable_class(FileDirDict)