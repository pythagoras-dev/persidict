"""EmptyDict: EmptyDict implementation that discards writes, always appears empty.

This module provides EmptyDict, a persistent dictionary that behaves like
/dev/null - accepting all writes but discarding them, and always appearing
empty on reads. Useful for testing, debugging, or as a no-op placeholder.
"""
from __future__ import annotations

from typing import Any, Iterator

from .safe_str_tuple import NonEmptySafeStrTuple
from .persi_dict import PersiDict, PersiDictKey, NonEmptyPersiDictKey


class EmptyDict(PersiDict):
    """
    An equivalent of the null device in OS - accepts all writes but discards them,
    returns nothing on reads. Always appears empty regardless of operations performed on it.
    
    This class is useful for testing, debugging, or as a placeholder when you want to
    disable persistent storage without changing the interface.

    Key characteristics:
    - All write operations are accepted, but data is discarded
    - All read operations behave as if the dict is empty
    - Length is always 0
    - Iteration always yields no results
    - Subdict operations return new EmptyDict instances
    - All timestamp operations raise KeyError (no data exists)

    Performance note: If validation is not needed, consider overriding __setitem__
    to simply pass for better performance.
    """
    
    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Always returns False as EmptyDict contains nothing."""
        return False


    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)


    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: str | None
                                 ) -> tuple[Any, str|None]:
        """Always raises KeyError as EmptyDict contains nothing.

        Args:
            key: Dictionary key (ignored, as EmptyDict has no items).
            etag: ETag value to compare against (ignored).

        Returns:
            tuple[Any, str|None]: Never returns as KeyError is always raised.

        Raises:
            KeyError: Always raised as EmptyDict contains no items.
        """
        raise KeyError(key)


    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any) -> None:
        """Accepts any write operation, discards the data (like /dev/null)."""
        # Run base validations (immutable checks, key normalization,
        # type checks, jokers) to ensure API consistency, then discard.
        self._process_setitem_args(key, value)
        # Do nothing - discard the write like /dev/null


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> str|None:
        """Accepts any write operation, discards the data, returns None as etag.

        Args:
            key: Dictionary key (processed for validation but discarded).
            value: Value to store (processed for validation but discarded).

        Returns:
            str|None: Always returns None as no actual storage occurs.

        Raises:
            KeyError: If attempting to modify when append_only is True.
            TypeError: If value doesn't match base_class_for_values when specified.
        """
        # Run base validations (immutable checks, key normalization,
        # type checks, jokers) to ensure API consistency, then discard.
        self._process_setitem_args(key, value)
        # Do nothing - discard the write like /dev/null

    
    def __delitem__(self, key: NonEmptyPersiDictKey) -> None:
        """Always raises KeyError as there's nothing to delete."""
        raise KeyError(key)


    def __len__(self) -> int:
        """Always returns 0 as EmptyDict is always empty."""
        return 0


    def __iter__(self) -> Iterator[PersiDictKey]:
        """Returns an empty iterator as EmptyDict contains no keys."""
        return iter(())


    def _generic_iter(self, result_type: set[str]) -> Iterator[tuple]:
        """Returns empty iterator for any generic iteration.

        Args:
            result_type: Set indicating desired fields among {'keys', 'values', 
                'timestamps'}. Validated but result is always empty.

        Returns:
            Iterator[tuple]: Always returns an empty iterator.

        Raises:
            ValueError: If result_type is invalid or contains unsupported fields.
        """
        self._process_generic_iter_args(result_type)
        return iter(())


    def clear(self) -> None:
        """No-op since EmptyDict is always empty."""
        pass


    def get(self, key: NonEmptyPersiDictKey, default: Any = None) -> Any:
        """Always returns the default value since key is never found."""
        return default


    def setdefault(self, key: NonEmptyPersiDictKey, default: Any = None) -> Any:
        """Always returns the default value without storing it."""
        return default


    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)


    def discard(self, key: NonEmptyPersiDictKey) -> bool:
        """Always returns False as the key never exists."""
        return False

    def delete_if_exists(self, key: NonEmptyPersiDictKey) -> bool:
        """Backward-compatible wrapper for discard()."""
        return self.discard(key)


    def random_key(self) -> NonEmptySafeStrTuple|None:
        """Returns None as EmptyDict contains no keys."""
        return None


    def get_params(self) -> dict[str, Any]:
        """Return parameters for this EmptyDict."""
        params = super().get_params()
        return params


    def get_subdict(self, prefix_key: PersiDictKey) -> 'EmptyDict':
        """Returns a new EmptyDict as subdictionary.

        Args:
            prefix_key: Key prefix (ignored, as EmptyDict has no hierarchical structure).

        Returns:
            EmptyDict: A new EmptyDict instance with the same configuration.
        """
        return EmptyDict(**self.get_params())