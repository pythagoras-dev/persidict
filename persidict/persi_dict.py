""" Persistent dictionaries that store key-value pairs on local disks or AWS S3.

The module offers 3 main classes:

PersiDict: base class in the hierarchy, defines unified interface
of all persistent dictionaries.

FileDirDict (inherited from PersiDict) : a dictionary that
stores key-value pairs as files on a local hard-drive.
A key is used to compose a filename, while a value is stored
as a pickle or a json object in the file.

S3_Dict (inherited from PersiDict): a dictionary that
stores key-value pairs on AWS S3.
A key is used to compose an objectname, while a value is stored
as a pickle or a json S3 object.
"""
from __future__ import annotations

import base64
import hashlib
import os
import pickle
from abc import *
from typing import Any, Tuple, Union, Sequence
import string

import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas

from collections.abc import MutableMapping

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()

PersiDictKey = Union[ str, Sequence[str]]
""" A value which can be used as a key for PersiDict. 

PersiDictKey must be a string or a sequence of strings.
The characters within strings are restricted to SAFE_CHARS set.
"""

SAFE_CHARS = set(string.ascii_letters + string.digits + "()_-~.=")

class PersiDict(MutableMapping):
    """Dict-like durable store that accepts sequences of strings as keys.

    An abstract base class for key-value stores. It accepts keys in a form of
    either a single sting or a sequence (tuple, list, etc.) of strings.
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

    min_digest_len : int
                 Length of a hash signature suffix which SimplePersistentDict
                 automatically adds to each string in a key
                 while mapping the key to an address of a value
                 in a persistent storage backend (e.g. a filename
                 or an S3 objectname). We need it to ensure correct work
                 of persistent dictionaries with case-insensitive
                 (even if case-preserving) filesystems, such as MacOS HFS.

    """
    # TODO: refactor to support variable length of min_digest_len
    min_digest_len:int
    immutable_items:bool

    def __init__(self
                 , immutable_items:bool
                 , min_digest_len:int = 8
                 , **kwargas):
        assert min_digest_len >= 0
        self.min_digest_len = min_digest_len
        self.immutable_items = bool(immutable_items)


    def _create_suffix(self, input_str:str) -> str:
        """ Create a hash signature suffix for a string."""

        assert isinstance(input_str, str)

        if self.min_digest_len == 0:
            return ""

        input_b = input_str.encode()
        hash_object = hashlib.md5(input_b)
        full_digest_str = base64.b32encode(hash_object.digest()).decode()
        suffix = "_" + full_digest_str[:self.min_digest_len]

        return suffix


    def _add_suffix_if_absent(self, input_str:str) -> str:
        """ Add a hash signature suffix to a string if it's not there."""

        assert isinstance(input_str, str)

        if self.min_digest_len == 0:
            return input_str

        if len(input_str) > self.min_digest_len + 1:
            possibly_already_present_suffix = self._create_suffix(
                input_str[:-1-self.min_digest_len])
            if input_str.endswith(possibly_already_present_suffix):
                return input_str

        return input_str + self._create_suffix(input_str)


    def _remove_suffix_if_present(self, input_str:str) -> str:
        """ Remove a hash signature suffix from a string if it's detected."""

        assert isinstance(input_str, str)

        if self.min_digest_len == 0:
            return input_str

        if len(input_str) > self.min_digest_len + 1:
            possibly_already_present_suffix = self._create_suffix(
                input_str[:-1-self.min_digest_len])
            if input_str.endswith(possibly_already_present_suffix):
                return input_str[:-1-self.min_digest_len]

        return input_str


    def _remove_all_suffixes_if_present(self, key:PersiDictKey) -> PersiDictKey:
        """ Remove hash signature suffixes from all strings in a key."""

        if self.min_digest_len == 0:
            return key

        if isinstance(key, str):
            key = (key,)

        new_key = []
        for sub_key in key:
            new_sub_key = self._remove_suffix_if_present(sub_key)
            new_key.append(new_sub_key)

        new_key = tuple(new_key)

        return new_key


    def _normalize_key(self, key:PersiDictKey) -> Tuple[str,...]:
        """Check if a key meets requirements and return its standardized form.

        A key must be either a string or a sequence of non-empty strings.
        If it is a single string, it will be transformed into a tuple,
        consisting of this sole string. During the transformation,
        each string will also get a hash-based suffix (a signature).

        Each string in an input  sequence can contain
        only URL-safe characters (alphanumerical characters
        and characters from this list: ()_-.=)
        """

        try:
            iter(key)
        except:
            raise KeyError(f"Key must be a string or a sequence of strings.")
        if isinstance(key, str):
            key = (key,)
        for s in key:
            assert isinstance(s,str), (
                    "Key must be a string or a sequence of strings.")
            assert len(set(s) - SAFE_CHARS) == 0, (
                f"Invalid characters in the key: {(set(s) - SAFE_CHARS)}"
                + "\nOnly the following chars are allowed in a key:"
                + "".join(list(SAFE_CHARS)))
            assert len(s), "Only non-empty strings are allowed in a key"

        new_key = []
        for s in key:
            new_key.append(self._add_suffix_if_absent(s))

        new_key = tuple(new_key)

        return new_key


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


    def __ne__(self, other) -> bool:
        """Return self!=value. """
        return not (self == other)


    @abstractmethod
    def mtimestamp(self,key:PersiDictKey) -> float:
        """Get last modification time (in seconds, Unix epoch time).

        This method is absent in the original dict API.
        """
        raise NotImplementedError


    def clear(self) -> None:
        """Remove all items from the dictionary. """
        for k in self.keys():
            del self[k]


    def safe_delete(self, key:PersiDictKey):
        """ Delete an item without raising an exception if it doesn't exist.

        This method is absent in the original dict API, it is added here
        to minimize network calls for (remote) persistent dictionaries.
        """

        if self.immutable_items: # TODO: change to exceptions
            assert False, "Can't delete an immutable key-value pair"

        try:
            self.__delitem__(key)
        except:
            pass


    def get_subdict(self, prefix_key:PersiDictKey) -> PersiDict:
        """Get a subdictionary containing items with the same prefix_key.

        This method is absent in the original dict API.
        """
        raise NotImplementedError