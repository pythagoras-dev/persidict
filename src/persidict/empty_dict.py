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


    def get_item_if_new_etag(self, key: NonEmptyPersiDictKey, etag: str|None
                             ) -> tuple[Any, str]:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)


    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any) -> None:
        """Accepts any write operation, discards the data (like /dev/null)."""
        # Run base validations (immutable checks, key normalization,
        # type checks, jokers) to ensure API consistency, then discard.
        super().__setitem__(key, value)
        # Do nothing - discard the write like /dev/null


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> str|None:
        """Accepts any write operation, discards the data, returns None as etag."""
        # Run base validations (immutable checks, key normalization,
        # type checks, jokers) to ensure API consistency, then discard.
        super().set_item_get_etag(key, value)
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
        """Returns empty iterator for any generic iteration."""
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


    def delete_if_exists(self, key: NonEmptyPersiDictKey) -> bool:
        """Always returns False as the key never exists."""
        return False


    def random_key(self) -> NonEmptySafeStrTuple|None:
        """Returns None as EmptyDict contains no keys."""
        return None


    def get_params(self) -> dict[str, Any]:
        """Return parameters for this EmptyDict."""
        params = super().get_params()
        return params


    def base_dir(self) -> str:
        """Returns empty string as there's no storage directory."""
        return ""


    def base_url(self) -> str:
        """Returns empty string as there's no storage URL."""
        return ""


    def get_subdict(self, prefix_key: PersiDictKey) -> 'EmptyDict':
        """Returns a new EmptyDict as subdictionary."""
        return EmptyDict(**self.get_params())