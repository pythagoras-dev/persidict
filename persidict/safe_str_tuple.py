"""SafeStrTuple: an immutable flat tuple of non-emtpy URL/filename-safe strings.
"""

import string
from collections.abc import Sequence, Mapping, Hashable
from typing import Tuple, Any

SAFE_CHARS_SET = set(string.ascii_letters + string.digits + "()_-~.=")

def _is_sequence_not_mapping(obj:Any)->bool:
    """Check if obj is a sequence (e.g. list) but not a mapping (e.g. dict)."""
    if isinstance(obj, Sequence) and not isinstance(obj, Mapping):
        return True
    elif hasattr(obj, "keys") and callable(obj.keys):
        return False
    elif (hasattr(obj, "__getitem__") and callable(obj.__getitem__)
        and hasattr(obj, "__len__") and callable(obj.__len__)
        and hasattr(obj, "__iter__") and callable(obj.__iter__)):
        return True
    else:
        return False

class SafeStrTuple(Sequence, Hashable):
    """An immutable sequence of non-emtpy URL/filename-safe strings.
    """

    flat_str_chain: Tuple[str, ...]

    def __init__(self, *args):
        """Create a SafeStrTuple from a sequence/tree of strings.

        The constructor accepts a sequence (list, tuple, etc.) of objects,
        each of which can be a string or a nested sequence of
        objects with similar structure. The input tree of strings is flattened.
        Each string must be non-empty and contain
        only URL/filename-safe characters.
        """
        assert len(args) > 0
        candidate_str_chain = []
        for a in args:
            if isinstance(a, SafeStrTuple):
                candidate_str_chain.extend(a.flat_str_chain)
            elif isinstance(a, str):
                assert len(a) > 0
                assert len(set(a) - SAFE_CHARS_SET) == 0
                candidate_str_chain.append(a)
            elif _is_sequence_not_mapping(a):
                if len(a) > 0:
                    candidate_str_chain.extend(SafeStrTuple(*a).flat_str_chain)
            else:
                assert False, f"Invalid argument type: {type(a)}"
        self.flat_str_chain = tuple(candidate_str_chain)

    def __getitem__(self, key):
        """Return a string at position key."""
        return self.flat_str_chain[key]

    def __len__(self):
        """Return the number of strings in the tuple."""
        return len(self.flat_str_chain)

    def __hash__(self):
        """Return a hash of the tuple."""
        return hash(self.flat_str_chain)

    @classmethod
    def allowed_characters(cls):
        """Return a set of allowed characters."""
        return SAFE_CHARS_SET

    def __repr__(self):
        """Return repr(self)."""
        return f"SafeStrSequence({self.flat_str_chain})"


    def __eq__(self, other):
        """Return self == other."""
        assert isinstance(other, SafeStrTuple)
        return self.flat_str_chain == other.flat_str_chain


    def __add__(self, other):
        """Return self + other."""
        other = SafeStrTuple(other)
        return SafeStrTuple(*(self.flat_str_chain + other.flat_str_chain))

    def __radd__(self, other):
        """Return other + self."""
        other = SafeStrTuple(other)
        return SafeStrTuple(*(other.flat_str_chain + self.flat_str_chain))

    def __iter__(self):
        """Return iter(self)."""
        return iter(self.flat_str_chain)

    def __contains__(self, item):
        """Return item in self."""
        return item in self.flat_str_chain

    def __reversed__(self):
        """Return a reversed SafeStrTuple."""
        return SafeStrTuple(*reversed(self.flat_str_chain))