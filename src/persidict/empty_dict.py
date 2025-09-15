from __future__ import annotations

from typing import Any, Iterator
from parameterizable import register_parameterizable_class

from .persi_dict import PersiDict, PersiDictKey


class EmptyDict(PersiDict):
    """
    An equivalent of the null device in OS - accepts all writes but discards them,
    returns nothing on reads. Always appears empty regardless of operations performed on it.
    
    This class is useful for testing, debugging, or as a placeholder when you want to
    disable persistent storage without changing the interface.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize an EmptyDict that behaves like a null device."""
        # Call parent constructor but ignore most parameters since we don't store anything
        super().__init__(*args, **kwargs)
    
    def __contains__(self, key: PersiDictKey) -> bool:
        """Always returns False as EmptyDict contains nothing."""
        return False
    
    def __getitem__(self, key: PersiDictKey) -> Any:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)
    
    def __setitem__(self, key: PersiDictKey, value: Any) -> None:
        """Accepts any write operation but discards the data (null device behavior)."""
        # Do nothing - discard the write like /dev/null
        pass
    
    def __delitem__(self, key: PersiDictKey) -> None:
        """Always raises KeyError as there's nothing to delete."""
        raise KeyError(key)
    
    def __len__(self) -> int:
        """Always returns 0 as EmptyDict is always empty."""
        return 0
    
    def __iter__(self) -> Iterator[PersiDictKey]:
        """Returns empty iterator as EmptyDict contains no keys."""
        return iter([])
    
    def _generic_iter(self, result_type: set[str]) -> Iterator[tuple]:
        """Returns empty iterator for any generic iteration."""
        return iter([])
    
    def clear(self) -> None:
        """No-op since EmptyDict is always empty."""
        pass
    
    def get(self, key: PersiDictKey, default: Any = None) -> Any:
        """Always returns the default value since key is never found."""
        return default
    
    def setdefault(self, key: PersiDictKey, default: Any = None) -> Any:
        """Always returns the default value without storing it."""
        return default
    
    def timestamp(self, key: PersiDictKey) -> float:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)
    
    def delete_if_exists(self, key: PersiDictKey) -> bool:
        """Always returns False as the key never exists."""
        return False
    
    def random_key(self) -> PersiDictKey:
        """Always raises KeyError as EmptyDict contains no keys."""
        raise KeyError("EmptyDict contains no keys")
    
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