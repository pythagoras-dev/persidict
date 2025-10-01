"""Persistent, dict-like API for durable key-value stores.

PersiDict defines a unified interface for persistent dictionaries. The API is
similar to Python's built-in dict with some differences (e.g., insertion order
is not guaranteed) and several additional convenience methods.

Keys are non-empty sequences of URL/filename-safe strings
represented by SafeStrTuple. Plain strings or sequences of strings are accepted
and automatically coerced to SafeStrTuple. Values can be
arbitrary Python objects unless an implementation restricts them
via `base_class_for_values`.

Persistence means items are stored durably (e.g., in local files or cloud
objects) and remain accessible across process lifetimes.
"""

from __future__ import annotations

from abc import abstractmethod
import heapq
import random
from parameterizable import ParameterizableClass, sort_dict_by_keys
from typing import Any, Sequence, Optional
from collections.abc import MutableMapping

from . import NonEmptySafeStrTuple
from .singletons import (KEEP_CURRENT, DELETE_CURRENT, Joker,
                         CONTINUE_NORMAL_EXECUTION, StatusFlag, EXECUTION_IS_COMPLETE,
                         ETagHasNotChangedFlag, ETAG_HAS_NOT_CHANGED)
from .safe_chars import contains_unsafe_chars
from .safe_str_tuple import SafeStrTuple

PersiDictKey = SafeStrTuple | Sequence[str] | str
NonEmptyPersiDictKey = NonEmptySafeStrTuple | Sequence[str] | str
"""A value which can be used as a key for PersiDict.

PersiDict instances accept keys in the form of (NonEmpty)SafeStrTuple,
or a string, or a (non-empty) sequence of strings.
The characters within strings must be URL/filename-safe.
If a string (or a sequence of strings) is passed to a PersiDict as a key,
it will be automatically converted into SafeStrTuple.
"""

class PersiDict(MutableMapping, ParameterizableClass):
    """Abstract dict-like interface for durable key-value stores.

    Keys are URL/filename-safe sequences of strings (SafeStrTuple). Concrete
    subclasses implement storage backends (e.g., filesystem, S3). The API is
    similar to Python's dict but does not guarantee insertion order and adds
    persistence-specific helpers (e.g., timestamp()).

    Attributes (can't be changed after initialization):
        append_only (bool):
            If True, items are immutable and non-removable: existing values 
            cannot be modified or deleted.
        base_class_for_values (Optional[type]):
            Optional base class that all values must inherit from. If None, any
            type is accepted.
        serialization_format (str):
            File extension/format for stored values (e.g., "pkl", "json").
    """

    append_only:bool
    base_class_for_values:Optional[type]
    serialization_format:str

    def __init__(self,
                 append_only: bool = False,
                 base_class_for_values: type|None = None,
                 serialization_format: str = "pkl",
                 *args, **kwargs):
        """Initialize base parameters shared by all persistent dictionaries.

        Args:
            append_only (bool): If True, items cannot be modified or deleted.
                Defaults to False.
            base_class_for_values (Optional[type]): Optional base class that values
                must inherit from. If None, values are not type-restricted.
                Defaults to None.
            serialization_format (str): File extension/format for stored values.
                Defaults to "pkl".
            *args: Additional positional arguments (ignored in base class, reserved
                for subclasses).
            **kwargs: Additional keyword arguments (ignored in base class, reserved
                for subclasses).

        Raises:
            ValueError: If serialization_format is an empty string,
            or contains unsafe characters, or not 'jason' or 'pkl'
            for non-string values.

            TypeError: If base_class_for_values is not a type or None.
        """
        
        self._append_only = bool(append_only)
        
        if len(serialization_format) == 0:
            raise ValueError("serialization_format must be a non-empty string")
        if contains_unsafe_chars(serialization_format):
            raise ValueError("serialization_format must contain only URL/filename-safe characters")
        self.serialization_format = str(serialization_format)

        if not isinstance(base_class_for_values, (type, type(None))):
            raise TypeError("base_class_for_values must be a type or None")
        if (base_class_for_values is None or
                not issubclass(base_class_for_values, str)):
            if serialization_format not in {"json", "pkl"}:
                raise ValueError("For non-string values serialization_format must be either 'pkl' or 'json'.")
        self.base_class_for_values = base_class_for_values

        ParameterizableClass.__init__(self)


    def get_params(self):
        """Return configuration parameters of this dictionary.

        Returns:
            dict: A sorted dictionary of parameters used to reconstruct the instance.
                This supports the Parameterizable API and is absent in the
                built-in dict.
        """
        params = dict(
            append_only=self.append_only,
            base_class_for_values=self.base_class_for_values,
            serialization_format=self.serialization_format
        )
        sorted_params = sort_dict_by_keys(params)
        return sorted_params

    def __copy__(self) -> 'PersiDict':
        """Return a shallow copy of the dictionary.

        This creates a new PersiDict instance with the same parameters, pointing
        to the same underlying storage. This is analogous to `dict.copy()`.

        Returns:
            PersiDict: A new PersiDict instance that is a shallow copy of this one.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot be copied directly")
        params = self.get_params()
        return self.__class__(**params)


    @property
    def append_only(self) -> bool:
        """Whether the store is append-only.

        Returns:
            bool: True if the store is append-only (contains immutable items
            that cannot be modified or deleted), False otherwise.
        """
        return self._append_only


    def __repr__(self) -> str:
        """Return a reproducible string representation.

        Returns:
            str: Representation including class name and constructor parameters.
        """
        params = self.get_params()
        params_str = ', '.join(f'{k}={v!r}' for k, v in params.items())
        return f'{self.__class__.__name__}({params_str})'


    def __str__(self) -> str:
        """Return a user-friendly string with all items.

        Returns:
            str: Stringified dict of items.
        """
        return str(dict(self.items()))


    @abstractmethod
    def __contains__(self, key:NonEmptyPersiDictKey) -> bool:
        """Check whether a key exists in the store.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if key exists, False otherwise.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                    " and cannot check items directly")


    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: str | None
                                 ) -> tuple[Any, str|None]|ETagHasNotChangedFlag:
        """Retrieve the value for a key only if its ETag has changed.

        This method is absent in the original dict API.
        By default, the timestamp is used in lieu of ETag.

        Args:
            key: Dictionary key (string or sequence of strings)
                or NonEmptySafeStrTuple.
            etag: The ETag value to compare against.

        Returns:
            tuple[Any, str|None] | ETagHasNotChangedFlag: The deserialized
                value if the ETag has changed, along with the new ETag,
                or ETAG_HAS_NOT_CHANGED if it matches the provided etag.

        Raises:
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        current_etag = self.etag(key)
        if etag == current_etag:
            return ETAG_HAS_NOT_CHANGED
        else:
            return self[key], current_etag


    @abstractmethod
    def __getitem__(self, key:NonEmptyPersiDictKey) -> Any:
        """Retrieve the value for a key.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            Any: The stored value.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot retrieve items directly")



    def _process_setitem_args(self, key: NonEmptyPersiDictKey, value: Any
                              ) -> StatusFlag:
        """Perform the first steps for setting an item.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True.
            TypeError: If the value is a PersiDict instance or does not match
                the required base_class_for_values when specified.

        Returns:
            StatusFlag: CONTINUE_NORMAL_EXECUTION if the caller should
                proceed with storing the value; EXECUTION_IS_COMPLETE if a
                joker command was processed and no further action is needed.
        """

        if value is KEEP_CURRENT:
            return EXECUTION_IS_COMPLETE
        elif self.append_only and (value is DELETE_CURRENT or key in self):
            raise KeyError("Can't modify an immutable key-value pair")
        elif isinstance(value, PersiDict):
            raise TypeError("Cannot store a PersiDict instance directly")

        key = NonEmptySafeStrTuple(key)

        if value is DELETE_CURRENT:
            self.discard(key)
            return EXECUTION_IS_COMPLETE

        if self.base_class_for_values is not None:
            if not isinstance(value, self.base_class_for_values):
                raise TypeError(f"Value must be an instance of"
                                f" {self.base_class_for_values.__name__}")

        return CONTINUE_NORMAL_EXECUTION


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> str|None:
        """Store a value for a key directly in the dict and return the new ETag.

        Handles special joker values (KEEP_CURRENT, DELETE_CURRENT) for
        conditional operations. Validates value types against base_class_for_values
        if specified, then serializes and uploads directly to S3.

        This method is absent in the original dict API.
        By default, the timestamp is used in lieu of ETag.

        Args:
            key: Dictionary key (string or sequence of strings)
                or NonEmptySafeStrTuple.
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Returns:
            str|None: The ETag of the newly stored object,
            or None if the ETag was not provided as a result of the operation.

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True.
            TypeError: If the value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """

        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None
        self[key] = value
        return self.etag(key)

    @abstractmethod
    def __setitem__(self, key:NonEmptyPersiDictKey, value:Any):
        """Set the value for a key.

        Special values KEEP_CURRENT and DELETE_CURRENT are interpreted as
        commands to keep or delete the current value respectively.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.
            value: Value to store, or a Joker command.

        Raises:
            KeyError: If attempting to modify an existing key when
                append_only is True.
            NotImplementedError: Subclasses must implement actual writing.
        """
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return

        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot store items directly")


    def _process_delitem_args(self, key: NonEmptyPersiDictKey) -> None:
        """Perform the first steps for deleting an item.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
        Raises:
            KeyError: If attempting to delete an item when
                append_only is True or if the key does not exist.
        """
        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        key = NonEmptySafeStrTuple(key)

        if key not in self:
            raise KeyError(f"Key {key} not found")


    @abstractmethod
    def __delitem__(self, key:NonEmptyPersiDictKey):
        """Delete a key and its value.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If append_only is True or if the key does not exist.
            NotImplementedError: Subclasses must implement deletion.
        """
        self._process_delitem_args(key)
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot delete items directly")


    @abstractmethod
    def __len__(self) -> int:
        """Return the number of stored items.

        Returns:
            int: Number of key-value pairs.
        Raises:
            NotImplementedError: Subclasses must implement counting.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                    " and cannot count items directly")


    def _process_generic_iter_args(self, result_type: set[str]) -> None:
        """Validate arguments for iterator helpers.

        Args:
            result_type: A set indicating desired fields among {'keys',
                'values', 'timestamps'}.
        Raises:
            TypeError: If result_type is not a set.
            ValueError: If result_type contains invalid entries or an invalid number of items.
        """
        if not isinstance(result_type, set):
            raise TypeError("result_type must be a set of strings")
        if not (1 <= len(result_type) <= 3):
            raise ValueError("result_type must contain between 1 and 3 elements")
        allowed = {"keys", "values", "timestamps"}
        if (result_type | allowed) != allowed:
            raise ValueError("result_type can only contain 'keys', 'values', 'timestamps'")
        if not (1 <= len(result_type & allowed) <= 3):
            raise ValueError("result_type must include at least one of 'keys', 'values', 'timestamps'")


    @abstractmethod
    def _generic_iter(self, result_type: set[str]) -> Any:
        """Underlying implementation for iterator helpers.

        Args:
            result_type: A set indicating desired fields among {'keys',
                'values', 'timestamps'}.

        Returns:
            Any: An iterator yielding keys, values, and/or timestamps based on
                result_type.

        Raises:
            TypeError: If result_type is not a set.
            ValueError: If result_type contains invalid entries or an invalid number of items.
            NotImplementedError: Subclasses must implement the concrete iterator.
        """
        self._process_generic_iter_args(result_type)
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot iterate items directly")


    def __iter__(self):
        """Iterate over keys.

        Returns:
            Iterator[SafeStrTuple]: Iterator of keys.
        """
        return self._generic_iter({"keys"})


    def keys(self):
        """Return an iterator over keys.

        Returns:
            Iterator[SafeStrTuple]: Keys iterator.
        """
        return self._generic_iter({"keys"})


    def keys_and_timestamps(self):
        """Return an iterator over (key, timestamp) pairs.

        Returns:
            Iterator[tuple[SafeStrTuple, float]]: Keys and POSIX timestamps.
        """
        return self._generic_iter({"keys", "timestamps"})


    def values(self):
        """Return an iterator over values.

        Returns:
            Iterator[Any]: Values iterator.
        """
        return self._generic_iter({"values"})


    def values_and_timestamps(self):
        """Return an iterator over (value, timestamp) pairs.

        Returns:
            Iterator[tuple[Any, float]]: Values and POSIX timestamps.
        """
        return self._generic_iter({"values", "timestamps"})


    def items(self):
        """Return an iterator over (key, value) pairs.

        Returns:
            Iterator[tuple[SafeStrTuple, Any]]: Items iterator.
        """
        return self._generic_iter({"keys", "values"})


    def items_and_timestamps(self):
        """Return an iterator over (key, value, timestamp) triples.

        Returns:
            Iterator[tuple[SafeStrTuple, Any, float]]: Items and timestamps.
        """
        return self._generic_iter({"keys", "values", "timestamps"})


    def setdefault(self, key: NonEmptyPersiDictKey, default: Any = None) -> Any:
        """Insert key with default value if absent; return the current value.

        Behaves like the built-in dict.setdefault() method: if the key exists,
        return its current value; otherwise, set the key to the default value
        and return that default.

        Args:
            key (PersiDictKey): Key (string, sequence of strings, or SafeStrTuple).
            default (Any): Value to insert if the key is not present. Defaults to None.

        Returns:
            Any: Existing value if key is present; otherwise the provided default value.

        Raises:
            TypeError: If default is a Joker command (KEEP_CURRENT/DELETE_CURRENT).
        """
        key = NonEmptySafeStrTuple(key)
        if isinstance(default, Joker):
            raise TypeError("default must be a regular value, not a Joker command")
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def __eq__(self, other: PersiDict) -> bool:
        """Compare dictionaries for equality.

        If other is a PersiDict instance, compares parameters for equality.
        Otherwise, attempts to compare as a mapping by comparing all keys and values.

        Args:
            other (PersiDict): Another dictionary-like object to compare against.

        Returns:
            bool: True if the dictionaries are considered equal, False otherwise.
        """
        try:
            if type(self) is type(other) :
                if self.get_params() == other.get_params():
                    return True
        except:
            pass

        try: #TODO: refactor to improve performance
            if len(self) != len(other):
                return False
            for k in other.keys():
                if self[k] != other[k]:
                    return False
            return True
        except KeyError:
            return False


    def __getstate__(self):
        """Prevent pickling of PersiDict instances.

        Raises:
            TypeError: Always raised; PersiDict instances are not pickleable.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot be pickled directly")


    def __setstate__(self, state):
        """Prevent unpickling of PersiDict instances.

        Raises:
            TypeError: Always raised; PersiDict instances are not pickleable.
        """
        if type(self) is PersiDict:
            raise TypeError("PersiDict is an abstract base class"
                            " and cannot be unpickled directly")


    def clear(self) -> None:
        """Remove all items from the dictionary.

        Raises:
            KeyError: If the dictionary is append-only.
        """
        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        for k in self.keys():
            try:
                del self[k]
            except KeyError:
                pass


    def discard(self, key: NonEmptyPersiDictKey) -> bool:
        """Delete an item without raising an exception if it doesn't exist.

        This method is absent in the original dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if the item existed and was deleted; False otherwise.

        Raises:
            KeyError: If the dictionary is append-only.
        """

        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        key = NonEmptySafeStrTuple(key)

        try:
            del self[key]
            return True
        except KeyError:
            return False


    def delete_if_exists(self, key: NonEmptyPersiDictKey) -> bool:
        """Backward-compatible wrapper for discard().

        This method is kept for backward compatibility; new code should use
        discard(). Behavior is identical to discard().
        """
        return self.discard(key)

    def get_subdict(self, prefix_key:PersiDictKey) -> PersiDict:
        """Get a sub-dictionary containing items with the given prefix key.

        Items whose keys start with the provided prefix are visible through the
        returned sub-dictionary. If the prefix does not exist, an empty
        sub-dictionary is returned. If the prefix is empty, the entire
        dictionary is returned.

        This method is absent in the original Python dict API.

        Args:
            prefix_key: Key prefix (string, sequence of strings, or SafeStrTuple)
                identifying the sub-dict to expose.

        Returns:
            PersiDict: A dictionary-like view restricted to keys under the
                provided prefix.

        Raises:
            NotImplementedError: Must be implemented by subclasses that support
                hierarchical key spaces.
        """

        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                " and cannot create sub-dictionaries directly")


    def subdicts(self) -> dict[str, PersiDict]:
        """Return a mapping of first-level keys to sub-dictionaries.

        This method is absent in the original dict API.

        Returns:
            dict[str, PersiDict]: A mapping from a top-level key segment to a
                sub-dictionary restricted to the corresponding keyspace.
        """
        all_keys = {k[0] for k in self.keys()}
        result_subdicts = {k: self.get_subdict(k) for k in all_keys}
        return result_subdicts


    def random_key(self) -> NonEmptySafeStrTuple | None:
        """Return a random key from the dictionary.

        This method is absent in the original Python dict API.

        Implementation uses reservoir sampling to select a uniformly random key
        in streaming time, without loading all keys into memory or using len().

        Returns:
            SafeStrTuple | None: A random key if the dictionary is not empty;
                None if the dictionary is empty.
        """
        iterator = iter(self.keys())
        try:
            # Get the first key
            result = next(iterator)
        except StopIteration:
            # Dictionary is empty
            return None

        # Reservoir sampling algorithm
        i = 2
        for key in iterator:
            # Select the current key with probability 1/i
            if random.random() < 1/i:
                result = key
            i += 1

        return result


    @abstractmethod
    def timestamp(self, key:NonEmptyPersiDictKey) -> float:
        """Return the last modification time of a key.

        This method is absent in the original dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            float: POSIX timestamp (seconds since Unix epoch) of the last
                modification of the item.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot provide timestamps directly")

    def etag(self, key:NonEmptyPersiDictKey) -> str|None:
        """Return the ETag of a key.

        By default, this returns a stringified timestamp of the last
        modification time. Subclasses may override to provide true
        backend-specific ETags (e.g., S3).

        This method is absent in the original Python dict API.
        """
        return f"{self.timestamp(key):.6f}"


    def oldest_keys(self, max_n=None) -> list[NonEmptySafeStrTuple]:
        """Return up to max_n oldest keys in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n (int | None): Maximum number of keys to return. If None,
                return all keys sorted by age (oldest first). Values <= 0
                yield an empty list. Defaults to None.

        Returns:
            list[SafeStrTuple]: The oldest keys, oldest first.
        """
        if max_n is None:
            # If we need all keys, sort them all by timestamp
            key_timestamp_pairs = list(self.keys_and_timestamps())
            key_timestamp_pairs.sort(key=lambda x: x[1])
            return [key for key,_ in key_timestamp_pairs]
        elif max_n <= 0:
            return []
        else:
            # Use heapq.nsmallest for efficient partial sorting without loading all keys into memory
            smallest_pairs = heapq.nsmallest(max_n,
                                             self.keys_and_timestamps(),
                                             key=lambda x: x[1])
            return [key for key,_ in smallest_pairs]


    def oldest_values(self, max_n=None) -> list[Any]:
        """Return up to max_n oldest values in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n (int | None): Maximum number of values to return. If None,
                return values for all keys sorted by age (oldest first). Values
                <= 0 yield an empty list.

        Returns:
            list[Any]: Values corresponding to the oldest keys.
        """
        return [self[k] for k in self.oldest_keys(max_n)]


    def newest_keys(self, max_n=None)  -> list[NonEmptySafeStrTuple]:
        """Return up to max_n newest keys in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n (int | None): Maximum number of keys to return. If None,
                return all keys sorted by age (newest first). Values <= 0
                yield an empty list. Defaults to None.

        Returns:
            list[SafeStrTuple]: The newest keys, newest first.
        """
        if max_n is None:
            # If we need all keys, sort them all by timestamp in reverse order
            key_timestamp_pairs = list(self.keys_and_timestamps())
            key_timestamp_pairs.sort(key=lambda x: x[1], reverse=True)
            return [key for key,_ in key_timestamp_pairs]
        elif max_n <= 0:
            return []
        else:
            # Use heapq.nlargest for efficient partial sorting without loading all keys into memory
            largest_pairs = heapq.nlargest(max_n,
                                           self.keys_and_timestamps(),
                                           key=lambda item: item[1])
            return [key for key,_ in largest_pairs]


    def newest_values(self, max_n=None) -> list[Any]:
        """Return up to max_n newest values in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n (int | None): Maximum number of values to return. If None,
                return values for all keys sorted by age (newest first). Values
                <= 0 yield an empty list.

        Returns:
            list[Any]: Values corresponding to the newest keys.
        """
        return [self[k] for k in self.newest_keys(max_n)]
