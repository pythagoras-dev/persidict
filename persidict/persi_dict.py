"""PersiDict: base class used in persistent dictionaries' hierarchy.

PersiDict: a base class in the hierarchy, defines unified interface
of all persistent dictionaries. The interface is similar to the interface of
Python's built-in Dict, with a few variations
(e.g. insertion order is not preserved) and additional methods.

PersiDict persistently stores key-value pairs.

A key is a sequence of strings in a form of SafeStrTuple.
Regular strings and their sequences can also be passed to PersiDict as keys,
in this case they will be automatically converted to SafeStrTuple.

A value can be any Python object.

'Persistently' means that key-value pairs are saved in a durable storage,
such as a local hard-drive or AWS S3 cloud, and can be retrieved
even after the Python process that created the dictionary has terminated.
"""

from __future__ import annotations

from abc import abstractmethod
import random
from typing import Any, Dict, Union, Sequence
from collections.abc import MutableMapping

from .safe_str_tuple import SafeStrTuple

PersiDictKey = Union[SafeStrTuple, Sequence[str], str]
""" A value which can be used as a key for PersiDict. 

PersiDict-s accept keys on a form of SafeStrTuple,
or a string, or a sequence of strings.
The characters within strings must be URL/filename-safe.
If a string (or a sequence of strings) is passed to a PersiDict as a key,
it will be automatically converted into SafeStrTuple.
"""

class PersiDict(MutableMapping):
    """Dict-like durable store that accepts sequences of strings as keys.

    An abstract base class for key-value stores. It accepts keys in a form of
    SafeStrSequence - a URL/filename-safe sequence of strings.
    It imposes no restrictions on types of values in the key-value pairs.

    The API for the class resembles the API of Python's built-in Dict
    (see https://docs.python.org/3/library/stdtypes.html#mapping-types-dict)
    with a few variations (e.g. insertion order is not preserved) and
    a few additional methods(e.g. .mtimestamp(key), which returns last
    modification time for a key).

    Attributes
    ----------
    immutable_items : bool
                      True means an append-only dictionary: items are
                      not allowed to be modified or deleted from a dictionary.
                      It enables various distributed cache optimizations
                      for remote storage.
                      False means normal dict-like behaviour.

    digest_len : int
                 Length of a hash signature suffix which PersiDict
                 automatically adds to each string in a key
                 while mapping the key to an address of a value
                 in a persistent storage backend (e.g. a filename
                 or an S3 objectname). We need it to ensure correct work
                 of persistent dictionaries with case-insensitive
                 (even if case-preserving) filesystems, such as MacOS HFS.

    """
    # TODO: refactor to support variable length of min_digest_len
    digest_len:int
    immutable_items:bool

    def __init__(self
                 , immutable_items:bool
                 , digest_len:int = 8
                 , *args, **kwargas):
        assert digest_len >= 0
        self.digest_len = int(digest_len)
        self.immutable_items = bool(immutable_items)


    def __repr__(self):
        """Return repr(self)"""
        repr_str = self.__class__.__name__ + "("
        repr_str += repr(dict(self.items()))
        repr_str += f", immutable_items={self.immutable_items}"
        repr_str += f", digest_len={self.digest_len}"
        repr_str += ")"
        return repr_str


    def __str__(self):
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
        if self.immutable_items: # TODO: change to exceptions
            assert key not in self, "Can't modify an immutable key-value pair"
        raise NotImplementedError


    def __delitem__(self, key:PersiDictKey):
        """Delete self[key]."""
        if self.immutable_items: # TODO: change to exceptions
            assert False, "Can't delete an immutable key-value pair"
        raise NotImplementedError


    @abstractmethod
    def __len__(self) -> int:
        """Return len(self)."""
        raise NotImplementedError


    @abstractmethod
    def _generic_iter(self, iter_type: str):
        """Underlying implementation for .items()/.keys()/.values() iterators"""
        assert iter_type in {"keys", "values", "items"}
        raise NotImplementedError


    def __iter__(self):
        """Implement iter(self)."""
        return self._generic_iter("keys")


    def keys(self):
        """D.keys() -> iterator object that provides access to D's keys"""
        return self._generic_iter("keys")


    def values(self):
        """D.values() -> iterator object that provides access to D's values"""
        return self._generic_iter("values")


    def items(self):
        """D.items() -> iterator object that provides access to D's items"""
        return self._generic_iter("items")


    def setdefault(self, key:PersiDictKey, default:Any=None) -> Any:
        """Insert key with a value of default if key is not in the dictionary.

        Return the value for key if key is in the dictionary, else default.
        """
        # TODO: check age cases to ensure the same semantics as standard dicts
        key = SafeStrTuple(key)
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default


    def __eq__(self, other) -> bool:
        """Return self==other. """
        try:
            if len(self) != len(other):
                return False
            for k in other.keys():
                if self[k] != other[k]:
                    return False
            return True
        except:
            return False


    @abstractmethod
    def mtimestamp(self, key:PersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.
        """
        raise NotImplementedError


    def clear(self) -> None:
        """Remove all items from the dictionary. """
        for k in self.keys():
            del self[k]


    def random_keys(self, max_n:int):
        """Return a list of random keys from the dictionary."""
        all_keys = list(self.keys())
        if max_n > len(all_keys):
            max_n = len(all_keys)
        result = random.sample(all_keys, max_n)
        return result


    def delete_if_exists(self, key:PersiDictKey) -> bool:
        """ Delete an item without raising an exception if it doesn't exist.

        Returns True if the item existed and was deleted, False otherwise.

        This method is absent in the original dict API.
        """

        if self.immutable_items: # TODO: change to exceptions
            assert False, "Can't delete an immutable key-value pair"

        key = SafeStrTuple(key)

        if key in self:
            del self[key]
            return True
        else:
            return False


    def get_subdict(self, prefix_key:PersiDictKey) -> PersiDict:
        """Get a sub-dictionary containing items with the same prefix_key.

        This method is absent in the original dict API.
        """
        raise NotImplementedError

    def subdicts(self) -> Dict[str, PersiDict]:
        """Get a dictionary of sub-dictionaries.

        This method is absent in the original dict API.
        """
        all_keys = {k[0] for k in self.keys()}
        result_subdicts = {k: self.get_subdict(k) for k in all_keys}
        return result_subdicts