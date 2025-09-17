"""Utilities for strict, flat tuples of URL/filename-safe strings.

This module defines SafeStrTuple, an immutable, hashable, flat tuple of non-empty
strings restricted to a predefined safe character set and bounded length. It is
useful for constructing keys and paths that must be portable and safe for URLs
and filesystems.
"""
from __future__ import annotations
from collections.abc import Sequence, Mapping, Hashable
from typing import Any
from .safe_chars import SAFE_CHARS_SET, SAFE_STRING_MAX_LENGTH


def _is_sequence_not_mapping(obj: Any) -> bool:
    """Return True if the object looks like a sequence but not a mapping.

    This function prefers ABC checks but falls back to duck-typing to handle
    some custom/typed collections.

    Args:
        obj: Object to inspect.

    Returns:
        bool: True if obj is a sequence (e.g., list, tuple) and not a mapping
        (e.g., dict); otherwise False.
    """
    if isinstance(obj, Sequence) and not isinstance(obj, Mapping):
        return True
    elif hasattr(obj, "keys") and callable(obj.keys):
        return False
    elif (
        hasattr(obj, "__getitem__")
        and callable(obj.__getitem__)
        and hasattr(obj, "__len__")
        and callable(obj.__len__)
        and hasattr(obj, "__iter__")
        and callable(obj.__iter__)
    ):
        return True
    else:
        return False


class SafeStrTuple(Sequence, Hashable):
    """An immutable sequence of URL/filename-safe strings.

    The sequence is flat (no nested structures) and hashable, making it suitable
    for use as a dictionary key. All strings are validated to contain only
    characters from SAFE_CHARS_SET and to have length less than
    SAFE_STRING_MAX_LENGTH.
    """

    strings: tuple[str, ...]

    def __init__(self, *args, **kwargs):
        """Initialize from strings or nested sequences of strings.

        The constructor accepts zero or more arguments which may be:
        - a SafeStrTuple
        - a single string
        - a sequence (list/tuple/etc.) containing any of the above recursively

        The input is flattened left-to-right into a single tuple of validated
        strings. Empty strings and strings with characters outside
        SAFE_CHARS_SET are rejected. Strings must also be shorter than
        SAFE_STRING_MAX_LENGTH.

        Args:
            *args: Zero or more inputs (strings, sequences, or SafeStrTuple) that
                will be flattened into a tuple of safe strings.
            **kwargs: Not supported.

        Raises:
            TypeError: If unexpected keyword arguments are provided, if no args
                are provided, or if an argument has an invalid type.
            ValueError: If a string is empty, too long, or contains disallowed
                characters.
        """
        if len(kwargs) != 0:
            raise TypeError(f"Unexpected keyword arguments: {list(kwargs.keys())}")
        candidate_strings = []
        for a in args:
            if isinstance(a, SafeStrTuple):
                candidate_strings.extend(a.strings)
            elif isinstance(a, str):
                if len(a) == 0:
                    raise ValueError("Strings must be non-empty")
                if len(a) >= SAFE_STRING_MAX_LENGTH:
                    raise ValueError(
                        f"String length must be < {SAFE_STRING_MAX_LENGTH}, got {len(a)}")
                if not all(c in SAFE_CHARS_SET for c in a):
                    raise ValueError("String contains disallowed characters")
                candidate_strings.append(a)
            elif _is_sequence_not_mapping(a):
                if len(a) > 0:
                    candidate_strings.extend(SafeStrTuple(*a).strings)
            else:
                raise TypeError(f"Invalid argument type: {type(a)}")
        self.strings = tuple(candidate_strings)

    @property
    def str_chain(self) -> tuple[str, ...]:
        """Alias for strings for backward compatibility.

        Returns:
            tuple[str, ...]: The underlying tuple of strings.
        """
        return self.strings

    def __getitem__(self, key: int) -> str:
        """Return the string at the given index.

        Args:
            key: Zero-based index.

        Returns:
            str: The string at the specified position.
        """
        return self.strings[key]

    def __len__(self) -> int:
        """Return the number of strings in the tuple.

        Returns:
            int: The number of elements.
        """
        return len(self.strings)

    def __hash__(self):
        """Compute the hash of the underlying tuple.

        Returns:
            int: A hash value suitable for dict/set usage.
        """
        return hash(self.strings)

    def __repr__(self) -> str:
        """Return a developer-friendly representation.

        Returns:
            str: A representation including the class name and contents.
        """
        return f"{type(self).__name__}({self.strings})"

    def __eq__(self, other) -> bool:
        """Compare two SafeStrTuple-compatible objects for equality.

        If other is not a SafeStrTuple, it will be coerced using the same
        validation rules.

        Args:
            other: Another SafeStrTuple or compatible input.

        Returns:
            bool: True if both contain the same sequence of strings.
        """
        if isinstance(other, SafeStrTuple):
            if type(self).__eq__ != type(other).__eq__:
                return other.__eq__(self)
        else:
            other = SafeStrTuple(other)

        return self.strings == other.strings

    def __add__(self, other) -> SafeStrTuple:
        """Concatenate with another SafeStrTuple-compatible object.

        Args:
            other: Another SafeStrTuple or compatible input.

        Returns:
            SafeStrTuple: A new instance containing elements of self then other.
        """
        other = SafeStrTuple(other)
        return SafeStrTuple(*(self.strings + other.strings))

    def __radd__(self, other) -> SafeStrTuple:
        """Concatenate with another object in reversed order (other + self).

        Args:
            other: Another SafeStrTuple or compatible input.

        Returns:
            SafeStrTuple: A new instance containing elements of other then self.
        """
        other = SafeStrTuple(other)
        return SafeStrTuple(*(other.strings + self.strings))

    def __iter__(self):
        """Return an iterator over the strings.

        Returns:
            Iterator[str]: An iterator over the internal tuple.
        """
        return iter(self.strings)

    def __contains__(self, item) -> bool:
        """Check membership.

        Args:
            item: String to check for presence.

        Returns:
            bool: True if item is present.
        """
        return item in self.strings

    def __reversed__(self) -> SafeStrTuple:
        """Return a reversed SafeStrTuple.

        Returns:
            SafeStrTuple: A new instance with elements in reverse order.
        """
        return SafeStrTuple(*reversed(self.strings))

class NonEmptySafeStrTuple(SafeStrTuple):
    """A SafeStrTuple that must contain at least one string.

    This subclass enforces that the tuple is non-empty.
    """

    def __init__(self, *args, **kwargs):
        """Initialize and enforce non-empty constraint.

        Args:
            *args: One or more inputs (strings, sequences, or SafeStrTuple) that
                will be flattened into a tuple of safe strings.
            **kwargs: Not supported.

        Raises:
            TypeError: If unexpected keyword arguments are provided, if no args
                are provided, or if an argument has an invalid type.
            ValueError: If a string is empty, too long, contains disallowed
                characters, or if the resulting tuple is empty.
        """
        super().__init__(*args, **kwargs)
        if len(self.strings) == 0:
            raise ValueError("NonEmptySafeStrTuple must contain at least one string")