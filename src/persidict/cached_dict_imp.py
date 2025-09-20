from __future__ import annotations

from typing import Any, Optional

from .persi_dict import PersiDict, NonEmptyPersiDictKey
from .safe_str_tuple import NonEmptySafeStrTuple
from .singletons import ETAG_HAS_NOT_CHANGED, EXECUTION_IS_COMPLETE


class CachedDictImp(PersiDict):
    """A PersiDict implementation that composes a main dict and cache dicts.

    This class delegates truth to a main PersiDict while maintaining:
      - a data cache (PersiDict) for values, and
      - an optional ETag cache (PersiDict) for etag strings.

    If the main dict has immutable_items=True, the ETag cache is not needed
    because items can never change. In this case, set etag_cache to None.
    """

    def __init__(self,
                 main_dict: PersiDict,
                 data_cache: PersiDict,
                 etag_cache: Optional[PersiDict] = None) -> None:
        if not isinstance(main_dict, PersiDict):
            raise TypeError("main_dict must be a PersiDict")
        if not isinstance(data_cache, PersiDict):
            raise TypeError("data_cache must be a PersiDict")
        if etag_cache is not None and not isinstance(etag_cache, PersiDict):
            raise TypeError("etag_cache must be a PersiDict or None")
        if main_dict.immutable_items and etag_cache is not None:
            raise ValueError("etag_cache must be None when immutable_items=True")

        # Initialize PersiDict base with parameters mirroring the main dict so
        # that Parameterizable and runtime checks remain consistent.
        super().__init__(
            immutable_items=main_dict.immutable_items,
            digest_len=main_dict.digest_len,
            base_class_for_values=main_dict.base_class_for_values,
            file_type=main_dict.file_type,
        )

        self._main: PersiDict = main_dict
        self._data_cache: PersiDict = data_cache
        self._etag_cache: Optional[PersiDict] = etag_cache

    @property
    def prefix_key(self):
        # Keep the same logical namespace as the main dict
        return self._main.prefix_key

    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        key = NonEmptySafeStrTuple(key)
        if self.immutable_items and key in self._data_cache:
            return True
        else:
            return key in self._main

    def __len__(self) -> int:
        return len(self._main)

    def _generic_iter(self, result_type: set[str]):
        return self._main._generic_iter(result_type)

    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        return self._main.timestamp(key)

    def _get_cached_etag(self, key: NonEmptySafeStrTuple) -> Optional[str]:
        if self._etag_cache is None:
            return None
        else:
            return self._etag_cache.get(key, None)

    def _set_cached_etag(self, key: NonEmptySafeStrTuple, etag: Optional[str]) -> None:
        if self._etag_cache is None:
            return
        if etag is None:
            self._etag_cache.delete_if_exists(key)
        else:
            self._etag_cache[key] = etag

    def _etag_is_cached(self, key: NonEmptySafeStrTuple) -> bool:
        return self._etag_cache is not None and key in self._etag_cache

    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        key = NonEmptySafeStrTuple(key)
        if self.immutable_items:
            try:
                return self._data_cache[key]
            except KeyError:
                value =  self._main[key]
                self._data_cache[key] = value
                return value
        if self._etag_cache is None:
            return self._main[key]
        old_etag = self._etag_cache.get(key, None)
        res = self.get_item_if_new_etag(key, old_etag)
        if res == ETAG_HAS_NOT_CHANGED:
            try:
                return self._data_cache[key]
            except KeyError:
                value, _ =  self.get_item_if_new_etag(key, None)
                return value
        else:
            value, _ = res
            return value


    def get_item_if_new_etag(self, key: NonEmptyPersiDictKey, etag: Optional[str]):
        key = NonEmptySafeStrTuple(key)
        # Delegate to main, but keep caches in sync when a new value is fetched
        res = self._main.get_item_if_new_etag(key, etag)
        if res == ETAG_HAS_NOT_CHANGED:
            return res
        value, new_etag = res
        self._data_cache[key] = value
        self._set_cached_etag(key, new_etag)
        return res

    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any):
        # Reuse the base processing for jokers and type checks, but route actual
        # writes/deletes to the main dict and keep caches in sync via the
        # set_item_get_etag helper below.
        self.set_item_get_etag(key, value)

    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> Optional[str]:
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) == EXECUTION_IS_COMPLETE:
            return None
        # Let the main dict perform the write to determine the canonical result
        etag = self._main.set_item_get_etag(key, value)
        self._data_cache[key] = value
        self._set_cached_etag(key, etag)
        return etag

    def __delitem__(self, key: NonEmptyPersiDictKey):
        key = NonEmptySafeStrTuple(key)
        del self._main[key]
        self._data_cache.delete_if_exists(key)
        self._set_cached_etag(key, None)

    @property
    def base_url(self):
        return self._main.base_url

    @property
    def base_dir(self):
        return self._main.base_dir
