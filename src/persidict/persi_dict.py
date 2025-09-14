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

from .jokers import KEEP_CURRENT, DELETE_CURRENT, Joker
from .safe_str_tuple import SafeStrTuple

PersiDictKey = SafeStrTuple | Sequence[str] | str
"""A value which can be used as a key for PersiDict.

PersiDict instances accept keys in the form of SafeStrTuple,
or a string, or a sequence of strings.
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

    Attributes:
        immutable_items (bool):
            If True, items are write-once: existing values cannot be modified or
            deleted.
        digest_len (int):
            Length of a base32 MD5 digest fragment used to suffix each key
            component to avoid collisions on case-insensitive filesystems. 0
            disables suffixing.
        base_class_for_values (Optional[type]):
            Optional base class that all values must inherit from. If None, any
            type is accepted.
    """

    digest_len:int
    immutable_items:bool
    base_class_for_values:Optional[type]

    def __init__(self,
                 immutable_items: bool = False,
                 digest_len: int = 8,
                 base_class_for_values: Optional[type] = None,
                 *args, **kwargs):
        """Initialize base parameters shared by all persistent dictionaries.

        Args:
            immutable_items (bool): If True, items cannot be modified or deleted.
                Defaults to False.
            digest_len (int): Number of hash characters to append to key components
                to avoid case-insensitive collisions. Must be non-negative.
                Defaults to 8.
            base_class_for_values (Optional[type]): Optional base class that values
                must inherit from. If None, values are not type-restricted.
                Defaults to None.
            *args: Additional positional arguments (ignored in base class, reserved
                for subclasses).
            **kwargs: Additional keyword arguments (ignored in base class, reserved
                for subclasses).

        Raises:
            ValueError: If digest_len is negative.
        """
        self.digest_len = int(digest_len)
        if digest_len < 0:
            raise ValueError("digest_len must be non-negative")
        self.immutable_items = bool(immutable_items)
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
            immutable_items=self.immutable_items,
            digest_len=self.digest_len,
            base_class_for_values=self.base_class_for_values
        )
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    @abstractmethod
    def base_url(self):
        """Base URL identifying the storage location.

        Returns:
            str: A URL-like string (e.g., s3://bucket/prefix or file://...).

        Raises:
            NotImplementedError: Must be provided by subclasses.
        """
        raise NotImplementedError


    @property
    @abstractmethod
    def base_dir(self):
        """Base directory on the local filesystem, if applicable.

        Returns:
            str: Path to a local base directory used by the store.

        Raises:
            NotImplementedError: Must be provided by subclasses that use local
                storage.
        """
        raise NotImplementedError


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
    def __contains__(self, key:PersiDictKey) -> bool:
        """Check whether a key exists in the store.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if key exists, False otherwise.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                        " and cannot check items directly")


    @abstractmethod
    def __getitem__(self, key:PersiDictKey) -> Any:
        """Retrieve the value for a key.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            Any: The stored value.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                        " and cannot retrieve items directly")


    def __setitem__(self, key:PersiDictKey, value:Any):
        """Set the value for a key.

        Special values KEEP_CURRENT and DELETE_CURRENT are interpreted as
        commands to keep or delete the current value respectively.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.
            value: Value to store, or a Joker command.

        Raises:
            KeyError: If attempting to modify an existing key when
                immutable_items is True.
            NotImplementedError: Subclasses must implement actual writing.
        """
        if value is KEEP_CURRENT:
            return
        elif self.immutable_items:
            if key in self:
                raise KeyError("Can't modify an immutable key-value pair")
        elif value is DELETE_CURRENT:
            self.delete_if_exists(key)

        if self.base_class_for_values is not None:
            if not isinstance(value, self.base_class_for_values):
                raise TypeError(f"Value must be an instance of"
                                f" {self.base_class_for_values.__name__}")

        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot store items directly")


    def __delitem__(self, key:PersiDictKey):
        """Delete a key and its value.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If immutable_items is True.
            NotImplementedError: Subclasses must implement deletion.
        """
        if self.immutable_items:
            raise KeyError("Can't delete an immutable key-value pair")
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot delete items directly")
        key = SafeStrTuple(key)
        if key not in self:
            raise KeyError(f"Key {key} not found")


    @abstractmethod
    def __len__(self) -> int:
        """Return the number of stored items.

        Returns:
            int: Number of key-value pairs.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                        " and cannot count items directly")


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
        if not isinstance(result_type, set):
            raise TypeError("result_type must be a set of strings")
        if not (1 <= len(result_type) <= 3):
            raise ValueError("result_type must contain between 1 and 3 elements")
        allowed = {"keys", "values", "timestamps"}
        if (result_type | allowed) != allowed:
            raise ValueError("result_type can only contain 'keys', 'values', 'timestamps'")
        if not (1 <= len(result_type & allowed) <= 3):
            raise ValueError("result_type must include at least one of 'keys', 'values', 'timestamps'")
        if type(self) is PersiDict:
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


    def setdefault(self, key: PersiDictKey, default: Any = None) -> Any:
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
        key = SafeStrTuple(key)
        if isinstance(default, Joker):
            raise TypeError("default must be a regular value, not a Joker command")
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default


    def __eq__(self, other: PersiDict) -> bool:
        """Compare dictionaries for equality.

        If other is a PersiDict instance, compares portable parameters for equality.
        Otherwise, attempts to compare as a mapping by comparing all keys and values.

        Args:
            other (PersiDict): Another dictionary-like object to compare against.

        Returns:
            bool: True if the dictionaries are considered equal, False otherwise.
        """
        if isinstance(other, PersiDict):
            #TODO: decide whether to keep this semantics
            return self.get_portable_params() == other.get_portable_params()
        try:
            if len(self) != len(other):
                return False
            for k in other.keys():
                if self[k] != other[k]:
                    return False
            return True
        except:
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
            KeyError: If items are immutable (immutable_items is True).
        """
        if self.immutable_items:
            raise KeyError("Can't delete an immutable key-value pair")

        for k in self.keys():
            try:
                del self[k]
            except KeyError:
                pass


    def delete_if_exists(self, key:PersiDictKey) -> bool:
        """Delete an item without raising an exception if it doesn't exist.

        This method is absent in the original dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if the item existed and was deleted; False otherwise.

        Raises:
            KeyError: If items are immutable (immutable_items is True).
        """

        if self.immutable_items:
            raise KeyError("Can't delete an immutable key-value pair")

        key = SafeStrTuple(key)

        if key in self:
            try:
                del self[key]
                return True
            except:
                return False
        else:
            return False


    def get_subdict(self, prefix_key:PersiDictKey) -> PersiDict:
        """Get a sub-dictionary containing items with the given prefix key.

        Items whose keys start with the provided prefix are visible through the
        returned sub-dictionary. If the prefix does not exist, an empty
        sub-dictionary is returned.

        This method is absent in the original Python dict API.

        Args:
            prefix_key: Key prefix (string, sequence of strings, or SafeStrTuple)
                identifying the sub-namespace to expose.

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


    def random_key(self) -> PersiDictKey | None:
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
            # Select current key with probability 1/i
            if random.random() < 1/i:
                result = key
            i += 1

        return result


    @abstractmethod
    def timestamp(self, key:PersiDictKey) -> float:
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


    def oldest_keys(self, max_n=None):
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


    def oldest_values(self, max_n=None):
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


    def newest_keys(self, max_n=None):
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


    def newest_values(self, max_n=None):
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
