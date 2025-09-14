"""Persistent dictionary implementation backed by local files.

FileDirDict stores each key-value pair in a separate file under a base
directory. Keys determine directory structure and filename; values are
serialized depending on ``file_type``.

- file_type="pkl" or "json": arbitrary Python objects via pickle/jsonpickle.
- any other value: strings are stored as plain text.
"""
from __future__ import annotations

import os
import pathlib
import random
import tempfile
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
    text files (either in JSON format or as plain text).
    """

    _base_dir:str
    file_type:str

    def __init__(self
                 , base_dir: str = FILEDIRDICT_DEFAULT_BASE_DIR
                 , file_type: str = "pkl"
                 , immutable_items:bool = False
                 , digest_len:int = 8
                 , base_class_for_values: Optional[type] = None):
        """Initialize a filesystem-backed persistent dictionary.

        Args:
            base_dir (str): Base directory where all files are stored. Created
                if it does not exist.
            file_type (str): File extension/format to use for stored values.
                - "pkl" or "json": arbitrary Python objects are supported.
                - any other value: only strings are supported and stored as text.
            immutable_items (bool): If True, existing items cannot be modified
                or deleted.
            digest_len (int): Length of a hash suffix appended to each key path
                element to avoid case-insensitive collisions. Use 0 to disable.
                If you decide to enable it (not 0), we recommend at least 4.
            base_class_for_values (Optional[type]): Optional base class that all
                stored values must be instances of. If provided and not ``str``,
                then file_type must be either "pkl" or "json".

        Raises:
            ValueError: If file_type contains unsafe characters; or
                if configuration is inconsistent (e.g., non-str values
                with unsupported file_type).
            RuntimeError: If base_dir cannot be created or is not a directory.
        """

        super().__init__(immutable_items = immutable_items
                ,digest_len = digest_len
                ,base_class_for_values = base_class_for_values)

        if file_type != replace_unsafe_chars(file_type, ""):
            raise ValueError("file_type contains unsafe characters")
        self.file_type = file_type

        if (base_class_for_values is None or
                not issubclass(base_class_for_values,str)):
            if file_type not in {"json", "pkl"}:
                raise ValueError("For non-string values file_type must be either 'pkl' or 'json'.")

        base_dir = str(base_dir)

        if os.path.isfile(base_dir):
            raise ValueError(f"{base_dir} is a file, not a directory.")

        os.makedirs(base_dir, exist_ok=True)
        if not os.path.isdir(base_dir):
            raise RuntimeError(f"Failed to create or access directory: {base_dir}")

        # self.base_dir_param = _base_dir
        self._base_dir = os.path.abspath(base_dir)


    def get_params(self):
        """Return configuration parameters of the dictionary.

        This method is needed to support the Parameterizable API and is absent
        in the standard dict API.

        Returns:
            dict: A mapping of parameter names to values including base_dir and
                file_type merged with the base PersiDict parameters.
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

        Returns:
            str: URL of the underlying storage in the form "file://<abs_path>".
        """
        return pathlib.Path(self._base_dir).as_uri()



    @property
    def base_dir(self) -> str:
        """Return dictionary's base directory.

        This property is absent in the original dict API.

        Returns:
            str: Absolute path to the base directory used by this dictionary.
        """
        return self._base_dir


    def __len__(self) -> int:
        """Return the number of key-value pairs in the dictionary.

        This performs a recursive traversal of the base directory.

        Returns:
            int: Count of stored items.

        Note:
            This operation can be slow on large dictionaries as it walks the
            entire directory tree. Avoid using it in performance-sensitive
            code paths.
        """

        suffix = "." + self.file_type
        return sum(1 for _, _, files in os.walk(self._base_dir)
                   for f in files if f.endswith(suffix))


    def clear(self) -> None:
        """Remove all elements from the dictionary.

        Raises:
            KeyError: If immutable_items is True.
        """

        if self.immutable_items:
            raise KeyError("Can't clear a dict that contains immutable items")

        # we can't use shutil.rmtree() because
        # there may be overlapping dictionaries
        # with different file_type-s
        for subdir_info in os.walk(self._base_dir, topdown=False):
            (subdir_name, _, files) = subdir_info
            suffix = "." + self.file_type
            for f in files:
                if f.endswith(suffix):
                    os.remove(os.path.join(subdir_name, f))
            if (subdir_name != self._base_dir) and (
                    len(os.listdir(subdir_name)) == 0 ):
                try:
                    os.rmdir(subdir_name)
                except OSError:
                    # Directory is not empty, likely due to a race condition.
                    # Continue without raising an error.
                    pass


    def _build_full_path(self
                         , key:SafeStrTuple
                         , create_subdirs:bool=False
                         , is_file_path:bool=True) -> str:
        """Convert a key into an absolute filesystem path.

        Transforms a SafeStrTuple into either a directory path or a file path
        inside this dictionary's base directory. When is_file_path is True, the
        final component is treated as a filename with the configured file_type
        extension. When create_subdirs is True, missing intermediate directories
        are created.

        Args:
            key (SafeStrTuple): The key to convert. It will be temporarily
                signed according to digest_len to produce collision-safe names.
            create_subdirs (bool): If True, create any missing intermediate
                directories.
            is_file_path (bool): If True, return a file path ending with
                ".{file_type}"; otherwise return just the directory path for
                the key prefix.

        Returns:
            str: An absolute path within base_dir corresponding to the key.
        """

        key = sign_safe_str_tuple(key, self.digest_len)
        key = [self._base_dir] + list(key.strings)
        dir_names = key[:-1] if is_file_path else key

        if create_subdirs:
            dir_path = os.path.join(*dir_names)
            os.makedirs(dir_path, exist_ok=True)

        if is_file_path:
            file_name = key[-1] + "." + self.file_type
            return os.path.join(*dir_names, file_name)
        else:
            return str(os.path.join(*dir_names))


    def _build_key_from_full_path(self, full_path:str)->SafeStrTuple:
        """Convert an absolute filesystem path back into a SafeStrTuple key.

        This function reverses _build_full_path, stripping base_dir, removing the
        file_type extension if the path points to a file, and unsigning the key
        components according to digest_len.

        Args:
            full_path (str): Absolute path within the dictionary's base
                directory.

        Returns:
            SafeStrTuple: The reconstructed (unsigned) key.

        Raises:
            ValueError: If full_path is not located under base_dir.
        """

        # Ensure we're working with absolute paths
        full_path = os.path.abspath(full_path)

        # Remove the base directory from the path
        if not full_path.startswith(self._base_dir):
            raise ValueError(f"Path {full_path} is not within base directory {self._base_dir}")

        # Get the relative path
        rel_path = full_path[len(self._base_dir):].lstrip(os.sep)

        if not rel_path:
            return SafeStrTuple()

        # Split the path into components
        path_components = rel_path.split(os.sep)

        # If it's a file path, remove the file extension from the last component
        if os.path.isfile(full_path) and path_components[-1].endswith("." + self.file_type):
            path_components[-1] = path_components[-1][:-len("." + self.file_type)]

        # Create a SafeStrTuple from the path components
        key = SafeStrTuple(*path_components)

        # Unsign the key
        key = unsign_safe_str_tuple(key, self.digest_len)

        return key


    def get_subdict(self, key:PersiDictKey) -> FileDirDict:
        """Get a subdictionary containing items with the same prefix key.

        For non-existing prefix key, an empty sub-dictionary is returned.
        This method is absent in the original dict API.

        Args:
            key (PersiDictKey): Prefix key (string or sequence of strings) that
                identifies the subdirectory.

        Returns:
            FileDirDict: A new FileDirDict instance rooted at the specified
                subdirectory, sharing the same parameters as this dictionary.
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
        """Read a value from a single file without retries.

        Args:
            file_name (str): Absolute path to the file to read.

        Returns:
            Any: The deserialized value according to file_type.
        """

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
        """Read a value from a file with retry/backoff for concurrency.

        Validates that the configured file_type is compatible with the allowed
        value types, then attempts to read the file using an exponential backoff
        to better tolerate concurrent writers.

        Args:
            file_name (str): Absolute path of the file to read.

        Returns:
            Any: The deserialized value according to file_type.

        Raises:
            ValueError: If file_type is incompatible with non-string values.
            Exception: Propagates the last exception if all retries fail.
        """

        if not (self.file_type in {"pkl", "json"} or issubclass(
            self.base_class_for_values, str)):
            raise ValueError("When base_class_for_values is not str,"
                + " file_type must be pkl or json.")

        n_retries = 8
        # extra protections to better handle concurrent writes
        for i in range(n_retries):
            try:
                return self._read_from_file_impl(file_name)
            except Exception as e:
                if i < n_retries - 1:
                    time.sleep(random.uniform(0.01, 0.1) * (2 ** i))
                else:
                    raise e


    def _save_to_file_impl(self, file_name:str, value:Any) -> None:
        """Write a single value to a file atomically (no retries).

        Uses a temporary file and atomic rename to avoid partial writes and to
        reduce the chance of readers observing corrupted data.

        Args:
            file_name (str): Absolute destination file path.
            value (Any): Value to serialize and save.
        """

        dir_name = os.path.dirname(file_name)
        # Use a temporary file and atomic rename to prevent data corruption
        fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=".__tmp__")

        try:
            if self.file_type == "pkl":
                with open(fd, 'wb') as f:
                    joblib.dump(value, f, compress='lz4')
                    f.flush()
                    os.fsync(f.fileno())
            elif self.file_type == "json":
                with open(fd, 'w') as f:
                    f.write(jsonpickle.dumps(value, indent=4))
                    f.flush()
                    os.fsync(f.fileno())
            else:
                with open(fd, 'w') as f:
                    f.write(value)
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
            os.remove(temp_path)
            raise

    def _save_to_file(self, file_name:str, value:Any) -> None:
        """Save a value to a file with retry/backoff.

        Ensures the configured file_type is compatible with value types and then
        writes the value using an exponential backoff to better tolerate
        concurrent readers/writers.

        Args:
            file_name (str): Absolute destination file path.
            value (Any): Value to serialize and save.

        Raises:
            ValueError: If file_type is incompatible with non-string values.
            Exception: Propagates the last exception if all retries fail.
        """

        if not (self.file_type in {"pkl", "json"} or issubclass(
            self.base_class_for_values, str)):
            raise ValueError("When base_class_for_values is not str,"
                + " file_type must be pkl or json.")

        n_retries = 8
        # extra protections to better handle concurrent writes
        for i in range(n_retries):
            try:
                self._save_to_file_impl(file_name, value)
                return
            except Exception as e:
                if i < n_retries - 1:
                    time.sleep(random.uniform(0.01, 0.1) * (2 ** i))
                else:
                    raise e


    def __contains__(self, key:PersiDictKey) -> bool:
        """Check whether a key exists in the dictionary.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if a file for the key exists; False otherwise.
        """
        key = SafeStrTuple(key)
        filename = self._build_full_path(key)
        return os.path.isfile(filename)


    def __getitem__(self, key:PersiDictKey) -> Any:
        """Retrieve the value stored for a key.

        Equivalent to obj[key]. Reads the corresponding file from the disk and
        deserializes according to file_type.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            Any: The stored value.

        Raises:
            KeyError: If the file for the key does not exist.
            TypeError: If the deserialized value does not match base_class_for_values
                when it is set.
        """
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
        """Store a value for a key on the disk.

        Interprets joker values KEEP_CURRENT and DELETE_CURRENT accordingly.
        Validates value type if base_class_for_values is set, then serializes
        and writes to a file determined by the key and file_type.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.
            value (Any): Value to store, or a joker command.

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If the value is a PersiDict or does not match
                base_class_for_values when it is set.
        """

        key = SafeStrTuple(key)
        PersiDict.__setitem__(self, key, value)
        if isinstance(value, Joker):
            # processed by base class
            return

        filename = self._build_full_path(key, create_subdirs=True)
        self._save_to_file(filename, value)


    def __delitem__(self, key:PersiDictKey) -> None:
        """Delete the stored value for a key.

        Args:
            key (PersiDictKey): Key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If immutable_items is True or if the key does not exist.
        """
        key = SafeStrTuple(key)
        PersiDict.__delitem__(self, key)
        filename = self._build_full_path(key)
        if not os.path.isfile(filename):
            raise KeyError(f"File {filename} does not exist")
        os.remove(filename)


    def _generic_iter(self, result_type: set[str]):
        """Underlying implementation for .items()/.keys()/.values() iterators.

        Produces generators over keys, values, and/or timestamps by traversing
        the directory tree under base_dir. Keys are converted back from paths by
        removing the file extension and unsigning according to digest_len.

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
            TypeError: If result_type is not a set.
            ValueError: If result_type is empty or contains unsupported labels.
        """

        PersiDict._generic_iter(self, result_type)
        walk_results = os.walk(self._base_dir)
        ext_len = len(self.file_type) + 1

        def splitter(dir_path: str):
            """Transform a relative dirname into SafeStrTuple components.

            Args:
                dir_path (str): Relative path under base_dir (e.g., "a/b").

            Returns:
                list[str]: List of safe string components (may be empty).
            """
            if dir_path == ".":
                return []
            return dir_path.split(os.sep)

        def step():
            """Generator that yields entries based on result_type."""
            suffix = "." + self.file_type
            for dir_name, _, files in walk_results:
                for f in files:
                    if f.endswith(suffix):
                        prefix_key = os.path.relpath(
                            dir_name, start=self._base_dir)

                        result_key = (*splitter(prefix_key), f[:-ext_len])
                        result_key = SafeStrTuple(result_key)

                        to_return = []

                        if "keys" in result_type:
                            key_to_return= unsign_safe_str_tuple(
                                result_key, self.digest_len)
                            to_return.append(key_to_return)

                        if "values" in result_type:
                            full_path = os.path.join(dir_name, f)
                            value_to_return = self._read_from_file(full_path)
                            to_return.append(value_to_return)

                        if len(result_type) == 1:
                            yield to_return[0]
                        else:
                            if "timestamps" in result_type:
                                timestamp_to_return = os.path.getmtime(
                                    os.path.join(dir_name, f))
                                to_return.append(timestamp_to_return)
                            yield tuple(to_return)

        return step()


    def timestamp(self, key:PersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.

        Args:
            key (PersiDictKey): Key whose timestamp to return.

        Returns:
            float: POSIX timestamp of the underlying file.

        Raises:
            FileNotFoundError: If the key does not exist.
        """
        key = SafeStrTuple(key)
        filename = self._build_full_path(key)
        return os.path.getmtime(filename)


    def random_key(self) -> PersiDictKey | None:
        """Return a uniformly random key from the dictionary, or None if empty.

        Performs a full directory traversal using reservoir sampling
        (k=1) to select a random file matching the configured file_type without
        loading all keys into memory.

        Returns:
            PersiDictKey | None: A random key if any items exist; otherwise None.
        """
        # canonicalise extension once
        ext = None
        if self.file_type:
            ext = self.file_type.lower()
            if not ext.startswith("."):
                ext = "." + ext

        stack = [self._base_dir]
        winner: Optional[str] = None
        seen = 0

        while stack:
            path = stack.pop()
            try:
                with os.scandir(path) as it:
                    for ent in it:
                        if ent.is_dir(follow_symlinks=False):
                            stack.append(ent.path)
                            continue

                        # cheap name test before stat()
                        if ext and not ent.name.lower().endswith(ext):
                            continue

                        if ent.is_file(follow_symlinks=False):
                            seen += 1
                            if random.random() < 1 / seen:  # reservoir k=1
                                winner = ent.path
            except PermissionError:
                continue

        if winner is None:
            return None
        else:
            return self._build_key_from_full_path(os.path.abspath(winner))


parameterizable.register_parameterizable_class(FileDirDict)
