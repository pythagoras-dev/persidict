"""Persistent, dict-like API for durable key-value stores.

PersiDict defines a unified interface for persistent dictionaries. The API is
similar to Python's built-in dict with some differences (e.g., insertion order
is not guaranteed) and several additional convenience methods.

Keys are sequences of URL/filename-safe strings represented by SafeStrTuple.
Plain strings or sequences of strings are accepted and automatically coerced to
SafeStrTuple. Values can be arbitrary Python objects unless an implementation
restricts them via ``base_class_for_values``.

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
""" A value which can be used as a key for PersiDict. 

PersiDict-s accept keys on a form of SafeStrTuple,
or a string, or a sequence of strings.
The characters within strings must be URL/filename-safe.
If a string (or a sequence of strings) is passed to a PersiDict as a key,
it will be automatically converted into SafeStrTuple.
"""

class PersiDict(MutableMapping, ParameterizableClass):
    """Dict-like durable store that accepts sequences of strings as keys.

    An abstract base class for key-value stores. It accepts keys as
    URL/filename-safe sequences of strings (SafeStrTuple) and stores values in
    a persistent backend. Implementations may use local files, cloud objects,
    etc.

    The API resembles Python's built-in dict, with some differences (e.g.,
    insertion order is not preserved) and additional methods such as
    timestamp(key).

    Attributes:
        immutable_items (bool):
            If True, the dictionary is append-only: items cannot be modified
            or deleted. This can enable distributed cache optimizations for
            remote storage backends. If False, normal dict-like behavior.
        digest_len (int):
            Length of a hash signature suffix added to each string in a key
            when mapping keys to underlying storage addresses (e.g., filenames
            or S3 object names). Helps operate correctly on case-insensitive
            (even if case-preserving) filesystems.
        base_class_for_values (Optional[type]):
            Optional base class for values. If set, values must be instances of
            this type; otherwise, no type checks are enforced.
    """

    digest_len:int
    immutable_items:bool
    base_class_for_values:Optional[type]

    def __init__(self
                 , immutable_items:bool = False
                 , digest_len:int = 8
                 , base_class_for_values:Optional[type] = None
                 , *args, **kwargs):
        self.digest_len = int(digest_len)
        if digest_len < 0:
            raise ValueError("digest_len must be non-negative")
        self.immutable_items = bool(immutable_items)
        self.base_class_for_values = base_class_for_values
        ParameterizableClass.__init__(self)


    def get_params(self):
        """Return a dictionary of parameters for the PersiDict object.

        This method is needed to support Parameterizable API.
        The method is absent in the original dict API.
        """
        params =  dict(
            immutable_items=self.immutable_items
            , digest_len=self.digest_len
            , base_class_for_values=self.base_class_for_values
        )
        sorted_params = sort_dict_by_keys(params)
        return sorted_params


    @property
    @abstractmethod
    def base_url(self):
        """Return dictionary's URL

        This property is absent in the original dict API.
        """
        raise NotImplementedError


    @property
    @abstractmethod
    def base_dir(self):
        """Return dictionary's base directory in the local filesystem.

        This property is absent in the original dict API.
        """
        raise NotImplementedError


    def __repr__(self) -> str:
        """Return repr(self)"""
        params = self.get_params()
        params_str = ', '.join(f'{k}={v!r}' for k, v in params.items())
        return f'{self.__class__.__name__}({params_str})'


    def __str__(self) -> str:
        """Return str(self)"""
        return str(dict(self.items()))


    @abstractmethod
    def __contains__(self, key:PersiDictKey) -> bool:
        """True if the dictionary has the specified key, else False."""
        raise NotImplementedError


    @abstractmethod
    def __getitem__(self, key:PersiDictKey) -> Any:
        """X.__getitem__(y) is an equivalent to X[y]"""
        raise NotImplementedError


    def __setitem__(self, key:PersiDictKey, value:Any):
        """Set self[key] to value."""
        if value is KEEP_CURRENT:
            return
        elif value is DELETE_CURRENT:
            self.delete_if_exists(key)
        elif self.immutable_items:
            if key in self:
                raise KeyError("Can't modify an immutable key-value pair")
        raise NotImplementedError


    def __delitem__(self, key:PersiDictKey):
        """Delete self[key]."""
        if self.immutable_items: # TODO: change to exceptions
            raise KeyError("Can't delete an immutable key-value pair")
        raise NotImplementedError


    @abstractmethod
    def __len__(self) -> int:
        """Return len(self)."""
        raise NotImplementedError


    @abstractmethod
    def _generic_iter(self, result_type: set[str]) -> Any:
        """Underlying implementation for items/keys/values/... iterators"""
        assert isinstance(result_type, set)
        assert 1 <= len(result_type) <= 3
        assert len(result_type | {"keys", "values", "timestamps"}) == 3
        assert 1 <= len(result_type & {"keys", "values", "timestamps"}) <= 3
        raise NotImplementedError


    def __iter__(self):
        """Implement iter(self)."""
        return self._generic_iter({"keys"})


    def keys(self):
        """iterator object that provides access to keys"""
        return  self._generic_iter({"keys"})


    def keys_and_timestamps(self):
        """iterator object that provides access to keys and timestamps"""
        return self._generic_iter({"keys", "timestamps"})


    def values(self):
        """D.values() -> iterator object that provides access to D's values"""
        return self._generic_iter({"values"})


    def values_and_timestamps(self):
        """iterator object that provides access to values and timestamps"""
        return self._generic_iter({"values", "timestamps"})


    def items(self):
        """D.items() -> iterator object that provides access to D's items"""
        return self._generic_iter({"keys", "values"})


    def items_and_timestamps(self):
        """iterator object that provides access to keys, values, and timestamps"""
        return self._generic_iter({"keys", "values", "timestamps"})


    def setdefault(self, key:PersiDictKey, default:Any=None) -> Any:
        """Insert key with a value of default if key is not in the dictionary.

        Return the value for key if key is in the dictionary, else default.
        """
        # TODO: check edge cases to ensure the same semantics as standard dicts
        key = SafeStrTuple(key)
        assert not isinstance(default, Joker)
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default


    def __eq__(self, other) -> bool:
        """Return self==other. """
        if isinstance(other, PersiDict):
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
        raise TypeError("PersiDict is not picklable.")


    def __setstate__(self, state):
        raise TypeError("PersiDict is not picklable.")


    def clear(self) -> None:
        """Remove all items from the dictionary. """
        if self.immutable_items: # TODO: change to exceptions
            raise KeyError("Can't delete an immutable key-value pair")

        for k in self.keys():
            try:
                del self[k]
            except:
                pass


    def delete_if_exists(self, key:PersiDictKey) -> bool:
        """ Delete an item without raising an exception if it doesn't exist.

        Returns True if the item existed and was deleted, False otherwise.

        This method is absent in the original dict API.
        """

        if self.immutable_items: # TODO: change to exceptions
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
        """Get a sub-dictionary containing items with the same prefix key.

        For non-existing prefix key, an empty sub-dictionary is returned.

        This method is absent in the original Python dict API.
        """
        raise NotImplementedError


    def subdicts(self) -> dict[str, PersiDict]:
        """Get a dictionary of sub-dictionaries.

        This method is absent in the original dict API.
        """
        all_keys = {k[0] for k in self.keys()}
        result_subdicts = {k: self.get_subdict(k) for k in all_keys}
        return result_subdicts


    def random_key(self) -> PersiDictKey | None:
        """Return a random key from the dictionary.

        Returns a single random key if the dictionary is not empty.
        Returns None if the dictionary is empty.

        This method is absent in the original Python dict API.

        Implementation uses reservoir sampling to select a uniformly random key
        in streaming time, without loading all keys into memory or using len().
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
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.
        """
        raise NotImplementedError


    def oldest_keys(self, max_n=None):
        """Return max_n the oldest keys in the dictionary.

        If max_n is None, return all keys.

        This method is absent in the original Python dict API.
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
            smallest_pairs = heapq.nsmallest(max_n
                                             , self.keys_and_timestamps()
                                             , key=lambda x: x[1])
            return [key for key,_ in smallest_pairs]


    def oldest_values(self, max_n=None):
        """Return max_n the oldest values in the dictionary.

        If max_n is None, return all values.

        This method is absent in the original Python dict API.
        """
        return [self[k] for k in self.oldest_keys(max_n)]


    def newest_keys(self, max_n=None):
        """Return max_n the newest keys in the dictionary.

        If max_n is None, return all keys.

        This method is absent in the original Python dict API.
        """
        if max_n is None:
            # If we need all keys, sort them all by timestamp in reverse order
            key_timestamp_pairs = list(self.keys_and_timestamps())
            key_timestamp_pairs.sort(key=lambda x:x[1], reverse=True)
            return [key for key,_ in key_timestamp_pairs]
        elif max_n <= 0:
            return []
        else:
            # Use heapq.nlargest for efficient partial sorting without loading all keys into memory
            largest_pairs = heapq.nlargest(max_n
                                            , self.keys_and_timestamps()
                                            , key=lambda item: item[1])
            return [key for key,_ in largest_pairs]


    def newest_values(self, max_n=None):
        """Return max_n the newest values in the dictionary.

        If max_n is None, return all values.

        This method is absent in the original Python dict API.
        """
        return [self[k] for k in self.newest_keys(max_n)]
