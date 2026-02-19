"""Persistent dictionary implementation backed by local files.

FileDirDict stores each key-value pair in a separate file under a base
directory. Keys determine directory structure and filename; values are
serialized depending on ``serialization_format``.

- serialization_format="pkl" or "json": arbitrary Python objects via pickle/jsonpickle.
- any other value: strings are stored as plain text.
"""
from __future__ import annotations

import os
import random
import tempfile
import time
from typing import Any, Final

import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
from mixinforge import sort_dict_by_keys

from .jokers_and_status_flags import (
    Joker,
    EXECUTION_IS_COMPLETE,
    ETagValue,
)
from .safe_str_tuple import SafeStrTuple, NonEmptySafeStrTuple
from .safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple
from .persi_dict import PersiDict, PersiDictKey, NonEmptyPersiDictKey, ValueType
from .exceptions import MutationPolicyError, BackendError

if os.name == 'nt':
    import msvcrt
    import ctypes
    from ctypes import wintypes

    GENERIC_READ: Final[int] = 0x80000000
    FILE_SHARE_READ: Final[int] = 0x00000001
    FILE_SHARE_WRITE: Final[int] = 0x00000002
    FILE_SHARE_DELETE: Final[int] = 0x00000004
    OPEN_EXISTING: Final[int] = 3
    # Use unsigned form so the comparison matches wintypes.HANDLE (c_void_p)
    INVALID_HANDLE_VALUE: Final[int] = ctypes.c_void_p(-1).value

    CreateFileW = ctypes.windll.kernel32.CreateFileW
    CreateFileW.argtypes = [wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    CreateFileW.restype = wintypes.HANDLE

    CloseHandle = ctypes.windll.kernel32.CloseHandle
    CloseHandle.argtypes = [wintypes.HANDLE]
    CloseHandle.restype = wintypes.BOOL

    def add_long_path_prefix(path: str) -> str:
        """Add the '\\\\?\\' prefix to a path on Windows to support long paths.

        Handles both regular paths and UNC paths correctly.

        Args:
            path: The original file or directory path.

        Returns:
            The modified path with the appropriate prefix if on Windows
            and not already present; otherwise, the original path.
            UNC paths get '\\\\?\\UNC\\' prefix, regular paths get '\\\\?\\'.
        """
        if path.startswith('\\\\?\\'):
            return path
        elif path.startswith('\\\\'):
            # UNC path: \\server\share -> \\?\UNC\server\share
            return f'\\\\?\\UNC\\{path[2:]}'
        else:
            return f'\\\\?\\{path}'

    def drop_long_path_prefix(path: str) -> str:
        """Remove the '\\\\?\\' prefix from a path on Windows if present.

        Handles both regular paths and UNC paths correctly.

        Args:
            path: The file or directory path, possibly with the '\\\\?\\' prefix.

        Returns:
            The path without the '\\\\?\\' prefix if it was present; otherwise,
            the original path. UNC paths are converted back from '\\\\?\\UNC\\'
            format to '\\\\' format.
        """
        if path.startswith('\\\\?\\UNC\\'):
            # UNC path: \\?\UNC\server\share -> \\server\share
            return f'\\\\{path[8:]}'
        elif path.startswith('\\\\?\\'):
            return path[4:]
        else:
            return path

else:
    def add_long_path_prefix(path: str) -> str:
        """No-op on non-Windows platforms; returns path unchanged."""
        return path

    def drop_long_path_prefix(path: str) -> str:
        """No-op on non-Windows platforms; returns path unchanged."""
        return path

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()


class _InPlaceModificationError(Exception):
    """File was modified in-place during read (detected by fstat guard)."""


FILEDIRDICT_DEFAULT_BASE_DIR: Final[str] = "__file_dir_dict__"

class FileDirDict(PersiDict[ValueType]):
    """A persistent dict that stores key-value pairs in local files.

    A new file is created for each key-value pair.
    A key is either a filename (without an extension),
    or a sequence of directory names that ends with a filename.
    A value can be any Python object, which is stored in a file.
    Insertion order is not preserved.

    FileDirDict can store objects in binary files or in human-readable
    text files (either in JSON format or as plain text). By default, a
    short hash suffix (``digest_len=4``) is appended to each key path
    component to prevent collisions on case-insensitive filesystems.
    """

    _base_dir:str
    digest_len:int

    def __init__(self
                 , *
                 , base_dir: str = FILEDIRDICT_DEFAULT_BASE_DIR
                 , serialization_format: str = "pkl"
                 , append_only:bool = False
                 , digest_len:int = 4
                 , base_class_for_values: type | None = None):
        """Initialize a filesystem-backed persistent dictionary.

        Args:
            base_dir: Base directory where all files are stored. Created
                if it does not exist.
            serialization_format: File extension/format to use for stored values.
                - "pkl" or "json": arbitrary Python objects are supported.
                - any other value: only strings are supported and stored as text.
            append_only: If True, existing items cannot be modified
                or deleted.
            digest_len: Length of a hash suffix appended to each key path
                element to avoid case-insensitive collisions. Use 0 to disable.
            base_class_for_values: Optional base class that all
                stored values must be instances of. If provided and not ``str``,
                then serialization_format must be either "pkl" or "json".

        Raises:
            ValueError: If serialization_format contains unsafe characters; or
                if configuration is inconsistent (e.g., non-str values
                with unsupported serialization_format).
            RuntimeError: If base_dir cannot be created or is not a directory.
        """

        super().__init__(append_only=append_only,
                         base_class_for_values=base_class_for_values,
                         serialization_format=serialization_format)

        if digest_len < 0:
            raise ValueError("digest_len must be non-negative")
        self.digest_len = digest_len

        base_dir = str(base_dir)
        self._base_dir = os.path.abspath(base_dir)
        self._base_dir = add_long_path_prefix(self._base_dir)

        if os.path.isfile(self._base_dir):
            raise ValueError(f"{base_dir} is a file, not a directory.")

        os.makedirs(self._base_dir, exist_ok=True)
        if not os.path.isdir(self._base_dir):
            raise BackendError(
                f"Failed to create or access directory: {base_dir}",
                backend="filesystem", operation="init")


    def get_params(self) -> dict[str, Any]:
        """Return configuration parameters of the dictionary.

        This method is needed to support the ParameterizableMixin API and
        is absent in the standard dict API.

        Returns:
            A mapping of parameter names to values including base_dir
            merged with the base PersiDict parameters.
        """
        params = super().get_params()
        additional_params = dict(
            base_dir=self.base_dir,
            digest_len=self.digest_len)
        params= {**params, **additional_params}
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    def base_dir(self) -> str:
        """Return dictionary's base directory.

        This property is absent in the original dict API.

        Returns:
            Absolute path to the base directory used by this dictionary.
        """
        return drop_long_path_prefix(self._base_dir)


    def __len__(self) -> int:
        """Return the number of key-value pairs in the dictionary.

        This performs a recursive traversal of the base directory.

        Returns:
            Count of stored items.

        Note:
            This operation can be slow on large dictionaries as it walks the
            entire directory tree. Avoid using it in performance-sensitive
            code paths.
        """

        suffix = "." + self.serialization_format
        return sum(1 for _, _, files in os.walk(self._base_dir)
                   for f in files if f.endswith(suffix))


    def clear(self) -> None:
        """Remove all elements from the dictionary.

        Raises:
            MutationPolicyError: If append_only is True.
        """

        self._check_delete_policy()

        # we can't use shutil.rmtree() because
        # there may be overlapping dictionaries
        # with different serialization_format-s
        for subdir_info in os.walk(self._base_dir, topdown=False):
            (subdir_name, _, files) = subdir_info
            suffix = "." + self.serialization_format
            for f in files:
                if f.endswith(suffix):
                    try:
                        os.remove(os.path.join(subdir_name, f))
                    except OSError:
                        continue
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
        final component is treated as a filename with the configured serialization_format
        extension. When create_subdirs is True, missing intermediate directories
        are created.

        Args:
            key: The key to convert. It will be temporarily
                signed according to digest_len to produce collision-safe names.
            create_subdirs: If True, create any missing intermediate
                directories.
            is_file_path: If True, return a file path ending with
                ".{serialization_format}"; otherwise return just the directory path for
                the key prefix.

        Returns:
            An absolute path within base_dir corresponding to the key. On
            Windows, this path is prefixed with '\\\\?\\' to support paths
            longer than 260 characters.

        Raises:
            ValueError: If the resolved path escapes base_dir (path
                traversal defense-in-depth).
        """

        key = sign_safe_str_tuple(key, self.digest_len)
        key_components = [self._base_dir] + list(key.strings)
        dir_names = key_components[:-1] if is_file_path else key_components

        dir_path = str(os.path.join(*dir_names))

        if is_file_path:
            file_name = key_components[-1] + "." + self.serialization_format
            final_path = os.path.join(dir_path, file_name)
        else:
            final_path = dir_path

        # Defense-in-depth: verify that the resolved path stays
        # within base_dir to prevent path traversal attacks.
        normalised_base = os.path.normpath(
            drop_long_path_prefix(self._base_dir))
        normalised_path = os.path.normpath(
            drop_long_path_prefix(final_path))
        # Allow exact match (empty-prefix subdict) or proper child paths.
        # Use rstrip(os.sep) + os.sep to handle root dir correctly
        # (os.path.normpath("/") → "/" so "/" + "/" → "//", which breaks).
        base_prefix = normalised_base.rstrip(os.sep) + os.sep
        if normalised_path != normalised_base and not normalised_path.startswith(
                base_prefix):
            raise ValueError(
                f"Key resolves to a path outside base_dir: "
                f"{normalised_path}")

        if create_subdirs:
            path_for_makedirs = dir_path
            path_for_makedirs = add_long_path_prefix(path_for_makedirs)
            os.makedirs(path_for_makedirs, exist_ok=True)

        return add_long_path_prefix(final_path)


    def _build_key_from_full_path(self, full_path:str)->SafeStrTuple:
            """Convert an absolute filesystem path back into a SafeStrTuple key.

            This function reverses _build_full_path, stripping base_dir, removing the
            serialization_format extension if the path points to a file, and unsigning the key
            components according to digest_len.

            Args:
                full_path: Absolute path within the dictionary's base
                    directory.

            Returns:
                The reconstructed (unsigned) key.

            Raises:
                ValueError: If full_path is not located under base_dir.
            """

            # Remove the base directory from the path
            if not full_path.startswith(self._base_dir):
                raise ValueError(f"Path {full_path} is not "
                                 f"within base directory {self._base_dir}")

            # Get the relative path
            rel_path = os.path.relpath(
                drop_long_path_prefix(full_path),
                drop_long_path_prefix(self._base_dir))
            rel_path = os.path.normpath(rel_path)

            if not rel_path or rel_path == ".":
                return SafeStrTuple()

            # Split the path into components
            path_components = rel_path.split(os.sep)

            # If it's a file path, remove the file extension from the last component
            suffix = "." + self.serialization_format
            if path_components[-1].endswith(suffix):
                path_components[-1] = path_components[-1][:-len(suffix)]

            # Create a SafeStrTuple from the path components
            key = SafeStrTuple(*path_components)

            # Unsign the key
            key = unsign_safe_str_tuple(key, self.digest_len)

            return key


    def get_subdict(self, prefix_key:PersiDictKey) -> 'FileDirDict[ValueType]':
        """Get a subdictionary containing items with the same prefix key.

        For non-existing prefix key, an empty sub-dictionary is returned.
        If the prefix is empty, the entire dictionary is returned.
        This method is absent in the original dict API.

        Args:
            prefix_key: Prefix key (string or sequence of strings) that
                identifies the subdirectory.

        Returns:
            A new FileDirDict instance rooted at the specified
                subdirectory, sharing the same parameters as this dictionary.
        """
        prefix_key = SafeStrTuple(prefix_key)
        full_dir_path = self._build_full_path(
            prefix_key,
            create_subdirs = True,
            is_file_path = False)
        return FileDirDict(
            base_dir= full_dir_path
            , serialization_format=self.serialization_format
            , append_only= self.append_only
            , digest_len=self.digest_len
            , base_class_for_values=self.base_class_for_values)


    @staticmethod
    def _with_retry(fn, *args, n_retries=12,
                    retried_exceptions=(PermissionError,),
                    immediately_raised_exceptions=(), **kwargs):
        """Execute a callable with exponential backoff on transient errors.

        Args:
            fn: Callable to execute.
            *args: Positional arguments forwarded to *fn*.
            n_retries: Maximum number of attempts (default 12).
            retried_exceptions: Tuple of exception types that trigger a
                retry. Any exception not in this tuple is raised immediately.
            immediately_raised_exceptions: Tuple of exception types that
                are always raised immediately, even if they are subclasses
                of *retried_exceptions*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            The return value of *fn*.

        Raises:
            Exception: The last exception if all retries are exhausted, or
                any non-retried exception immediately.
        """
        for i in range(n_retries):
            try:
                return fn(*args, **kwargs)
            except immediately_raised_exceptions:
                raise
            except retried_exceptions:
                if i < n_retries - 1:
                    time.sleep(random.uniform(0.01, 0.2) * (1.75 ** i))
                else:
                    raise

    def _fstat_deserialize(
            self, f, file_name: str
            ) -> tuple[Any, os.stat_result]:
        """Deserialize from an open file with a double-fstat guard.

        Calls ``os.fstat`` before and after deserialization. If the two
        stats differ the file was modified in-place during the read, and
        ``_InPlaceModificationError`` is raised so that the caller (via
        ``_with_retry``) can retry.

        Args:
            f: An open file object with a valid ``.fileno()``.
            file_name: Path used only for the error message.

        Returns:
            ``(deserialized_value, stat_result)`` where *stat_result*
            is the ``os.fstat`` taken before the read.
        """
        stat_before = os.fstat(f.fileno())
        value = self._deserialize_from_file(f)
        stat_after = os.fstat(f.fileno())
        if self._etag_from_stat(stat_before) != self._etag_from_stat(stat_after):
            raise _InPlaceModificationError(file_name)
        return value, stat_before

    def _read_from_file_impl(
            self, file_name: str
            ) -> tuple[Any, os.stat_result]:
        """Read a value and its fstat from a single file without retries.

        Uses ``os.fstat`` on the open file descriptor so the returned
        stat always describes the exact bytes that were read.

        Args:
            file_name: Absolute path to the file to read.

        Returns:
            ``(deserialized_value, stat_result)``.

        Raises:
            FileNotFoundError: If *file_name* does not exist.
            _InPlaceModificationError: If the double-fstat guard
                detects that the file was modified during the read.
        """
        file_open_mode = 'rb' if self.serialization_format == "pkl" else 'r'
        file_encoding = None if self.serialization_format == "pkl" else "utf-8"
        if os.name == 'nt':
            handle = CreateFileW(file_name, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE, None, OPEN_EXISTING, 0, None)
            if int(handle) == INVALID_HANDLE_VALUE:
                error_code = ctypes.GetLastError()
                raise ctypes.WinError(error_code)

            fd = None
            try:
                if self.serialization_format == "pkl":
                    fd_open_mode = os.O_RDONLY | os.O_BINARY
                else:
                    fd_open_mode = os.O_RDONLY
                fd = msvcrt.open_osfhandle(int(handle),fd_open_mode)
            except Exception:
                CloseHandle(handle)
                raise

            try:
                f = os.fdopen(fd, file_open_mode, encoding=file_encoding)
                fd = None
            except Exception:
                if fd is not None:
                    os.close(fd)
                raise

            with f:
                return self._fstat_deserialize(f, file_name)
        else:
            with open(file_name, file_open_mode, encoding=file_encoding) as f:
                return self._fstat_deserialize(f, file_name)


    def _read_from_file(
            self, file_name: str
            ) -> tuple[Any, os.stat_result]:
        """Read a value and its fstat from a file, with retry/backoff.

        Retries on transient errors (e.g. ``PermissionError``,
        ``_InPlaceModificationError``) with exponential backoff.

        Args:
            file_name: Absolute path of the file to read.

        Returns:
            ``(deserialized_value, stat_result)`` where *stat_result*
            is the ``os.fstat`` of the open file descriptor.

        Raises:
            FileNotFoundError: Immediately if the file does not exist.
            _InPlaceModificationError: If the double-fstat guard
                consistently detects in-place modification after all
                retries are exhausted.
        """

        return self._with_retry(
            self._read_from_file_impl, file_name,
            retried_exceptions=(Exception,),
            immediately_raised_exceptions=(FileNotFoundError,))


    def _save_to_file_impl(self, file_name:str, value:Any) -> None:
        """Write a single value to a file atomically (no retries).

        Uses a temporary file and atomic rename to avoid partial writes and to
        reduce the chance of readers observing corrupted data.

        Args:
            file_name: Absolute destination file path.
            value: Value to serialize and save.
        """

        dir_name = os.path.dirname(file_name)
        # Use a temporary file and atomic rename to prevent data corruption
        fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=".__tmp__")

        try:
            file_open_mode = 'wb' if self.serialization_format == 'pkl' else 'w'
            file_encoding = None if self.serialization_format == 'pkl' else 'utf-8'
            with open(fd, file_open_mode, encoding=file_encoding) as f:
                self._serialize_to_file(value, f, pkl_compress='lz4')
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
                elif os.name == 'nt':
                    # On Windows, try to flush directory metadata
                    # This is less reliable than on POSIX systems
                    try:
                        handle = CreateFileW(
                            dir_name,
                            GENERIC_READ,
                            FILE_SHARE_READ | FILE_SHARE_WRITE,
                            None,
                            OPEN_EXISTING,
                            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS (needed for directories)
                            None
                        )
                        if int(handle) != INVALID_HANDLE_VALUE:
                            try:
                                kernel32 = ctypes.windll.kernel32
                                kernel32.FlushFileBuffers(handle)
                            finally:
                                CloseHandle(handle)
                    except Exception:
                        pass

            except Exception:
                pass

        except Exception:
            try:
                os.remove(temp_path)
            finally:
                raise

    def _save_to_file(self, file_name:str, value:Any) -> None:
        """Save a value to a file with retry/backoff.

        Args:
            file_name: Absolute destination file path.
            value: Value to serialize and save.

        Raises:
            Exception: Propagates the last exception if all retries fail.
        """

        self._with_retry(
            self._save_to_file_impl, file_name, value,
            retried_exceptions=(Exception,))


    def __contains__(self, key:NonEmptyPersiDictKey) -> bool:
        """Check whether a key exists in the dictionary.

        Args:
            key: Key (string or sequence of strings
                or NonEmptySafeStrTuple).

        Returns:
            True if a file for the key exists; False otherwise.
        """
        key = NonEmptySafeStrTuple(key)
        filename = self._build_full_path(key)
        return self._with_retry(os.path.isfile, filename)


    def __getitem__(self, key:NonEmptyPersiDictKey) -> ValueType:
        """Retrieve the value stored for a key.

        Equivalent to obj[key]. Reads the corresponding file from the disk and
        deserializes according to serialization_format.

        Args:
            key: Key (string or sequence of strings
                or NonEmptySafeStrTuple).

        Returns:
            The stored value.

        Raises:
            KeyError: If the file for the key does not exist.
            TypeError: If the deserialized value does not match base_class_for_values
                when it is set.
        """
        key = NonEmptySafeStrTuple(key)
        filename = self._build_full_path(key)
        try:
            result, _stat = self._read_from_file(filename)
        except FileNotFoundError as exc:
            raise KeyError(key) from exc
        self._validate_returned_value(result)
        return result

    def _get_value_and_etag(
            self, key: NonEmptySafeStrTuple,
            ) -> tuple[ValueType, ETagValue]:
        """Return a consistent value and ETag for a key.

        Uses ``os.fstat`` on the open file descriptor so the returned
        ETag is guaranteed to correspond to the exact bytes read.

        Args:
            key: Normalized dictionary key.

        Returns:
            A matching (value, ETag) pair.

        Raises:
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        filename = self._build_full_path(key)
        try:
            value, stat_result = self._read_from_file(filename)
        except FileNotFoundError as exc:
            raise KeyError(key) from exc
        self._validate_returned_value(value)
        return value, self._etag_from_stat(stat_result)


    def __setitem__(self, key:NonEmptyPersiDictKey, value: ValueType | Joker) -> None:
        """Store a value for a key on the disk.

        Interprets joker values KEEP_CURRENT and DELETE_CURRENT accordingly.
        Validates value type if base_class_for_values is set, then serializes
        and writes to a file determined by the key and serialization_format.

        When append_only is True, checks for key existence before writing
        (best-effort insert-if-absent). No file locking is performed, so
        concurrent writers may race on the same key.

        Args:
            key: Key (string or sequence of strings
                or NonEmptySafeStrTuple).
            value: Value to store, or a joker command.

        Raises:
            MutationPolicyError: If attempting to modify an existing item
                when append_only is True.
            TypeError: If the value is a PersiDict or does not match
                base_class_for_values when it is set.
        """

        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None

        filename = self._build_full_path(key, create_subdirs=True)

        if self.append_only:
            if key in self:
                raise MutationPolicyError("append-only")

        self._save_to_file(filename, value)


    def _remove_item(self, key: NonEmptySafeStrTuple) -> None:
        """Remove the file for *key* from disk.

        Raises:
            KeyError: If the file does not exist.
        """
        filename = self._build_full_path(key)
        try:
            self._with_retry(os.remove, filename)
        except FileNotFoundError as exc:
            raise KeyError(key) from exc

    def __delitem__(self, key:NonEmptyPersiDictKey) -> None:
        """Delete the stored value for a key.

        Args:
            key: Key (string or sequence of strings
                or NonEmptySafeStrTuple).

        Raises:
            MutationPolicyError: If append_only is True.
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        self._process_delitem_args(key)
        self._remove_item(key)


    def _generic_iter(self, result_type: set[str]):
        """Underlying implementation for .items()/.keys()/.values() iterators.

        Produces generators over keys, values, and/or timestamps by traversing
        the directory tree under base_dir. Keys are converted back from paths by
        removing the file extension and unsigning according to digest_len.

        Args:
            result_type: Any non-empty subset of {"keys", "values",
                "timestamps"} specifying which fields to yield.

        Returns:
            A generator yielding:
                - SafeStrTuple if result_type == {"keys"}
                - Any if result_type == {"values"}
                - tuple[SafeStrTuple, Any] if result_type == {"keys", "values"}
                - tuple[..., float] including POSIX timestamp if "timestamps" is requested.

        Raises:
            TypeError: If result_type is not a set, or if base_class_for_values
                is set and a yielded value does not match it.
            ValueError: If result_type is empty or contains unsupported labels.
        """

        self._process_generic_iter_args(result_type)
        walk_results = os.walk(self._base_dir)
        ext_len = len(self.serialization_format) + 1

        def splitter(dir_path: str):
            """Transform a relative dirname into SafeStrTuple components.

            Args:
                dir_path: Relative path under base_dir (e.g., "a/b").

            Returns:
                List of safe string components (may be empty).
            """
            if dir_path == ".":
                return []
            return dir_path.split(os.sep)

        def step():
            """Generator that yields entries based on result_type."""
            suffix = "." + self.serialization_format
            for dir_name, _, files in walk_results:
                for f in files:
                    if f.endswith(suffix):
                        prefix_key = os.path.relpath(
                            drop_long_path_prefix(dir_name),
                            start=drop_long_path_prefix(self._base_dir))

                        result_key = (*splitter(prefix_key), f[:-ext_len])
                        result_key = SafeStrTuple(result_key)

                        key_to_return = unsign_safe_str_tuple(
                            result_key, self.digest_len)

                        value_to_return = None
                        stat_result = None
                        if "values" in result_type:
                            # The file can be deleted between listing and fetching.
                            # Skip such races instead of raising to make iteration robust.
                            full_path = os.path.join(dir_name, f)
                            try:
                                value_to_return, stat_result = (
                                    self._read_from_file(full_path))
                            except Exception:
                                if not os.path.isfile(full_path):
                                    continue
                                else:
                                    raise
                            self._validate_returned_value(value_to_return)

                        timestamp_to_return = None
                        if "timestamps" in result_type:
                            if stat_result is not None:
                                timestamp_to_return = stat_result.st_mtime
                            else:
                                timestamp_to_return = os.path.getmtime(
                                    os.path.join(dir_name, f))

                        yield self._assemble_iter_result(
                            result_type
                            , key=key_to_return
                            , value=value_to_return
                            , timestamp=timestamp_to_return)

        return step()


    def timestamp(self, key:NonEmptyPersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.

        Args:
            key: Key whose timestamp to return.

        Returns:
            POSIX timestamp of the underlying file.

        Raises:
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        filename = self._build_full_path(key)
        try:
            return self._with_retry(os.path.getmtime, filename)
        except FileNotFoundError as exc:
            raise KeyError(key) from exc

    @staticmethod
    def _etag_from_stat(stat_result: os.stat_result) -> ETagValue:
        """Derive an ETag from an os.stat_result (mtime, size, inode).

        Including the inode detects atomic file replacements (write-to-temp
        + rename) where mtime and size could theoretically stay the same.
        """
        mtime_ns = getattr(stat_result, "st_mtime_ns", None)
        if mtime_ns is None:
            mtime_part = f"{stat_result.st_mtime:.6f}"
        else:
            mtime_part = str(mtime_ns)
        return ETagValue(f"{mtime_part}:{stat_result.st_size}:{stat_result.st_ino}")

    def etag(self, key:NonEmptyPersiDictKey) -> ETagValue:
        """Return a stable ETag derived from mtime, file size, and inode.

        Uses a single stat call and combines st_mtime_ns, st_size, and
        st_ino. Falls back to a float-based mtime representation if
        nanosecond precision is not available.

        Raises:
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        filename = self._build_full_path(key)
        try:
            stat_result = self._with_retry(os.stat, filename)
        except FileNotFoundError as exc:
            raise KeyError(key) from exc
        return self._etag_from_stat(stat_result)


    def random_key(self) -> NonEmptySafeStrTuple | None:
        """Return a uniformly random key from the dictionary, or None if empty.

        Performs a full directory traversal using reservoir sampling
        (k=1) to select a random file matching the configured serialization_format without
        loading all keys into memory.

        Returns:
            NonEmptySafeStrTuple | None: A random key if any items exist; otherwise None.
        """
        # canonicalise extension once
        ext = None
        if self.serialization_format:
            ext = self.serialization_format
            if not ext.startswith("."):
                ext = "." + ext

        stack = [self._base_dir]
        winner: str | None = None
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
                        if ext and not ent.name.endswith(ext):
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
            winner = os.path.abspath(winner)
            winner = add_long_path_prefix(winner)
            return self._build_key_from_full_path(winner)
