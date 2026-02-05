from __future__ import annotations

from typing import Optional

from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey, ValueType
from .safe_str_tuple import NonEmptySafeStrTuple, SafeStrTuple
from .jokers_and_status_flags import (ETAG_HAS_NOT_CHANGED, ETAG_HAS_CHANGED,
                                      EXECUTION_IS_COMPLETE, ETagHasChangedFlag,
                                      ETagHasNotChangedFlag, KEEP_CURRENT, DELETE_CURRENT,
                                      Joker)


class MutableDictCached(PersiDict[ValueType]):
    """PersiDict adapter with read-through caching and ETag validation.

    This adapter composes three concrete PersiDict instances:
    - main_dict: the source of truth that persists data and supports ETags.
    - data_cache: a PersiDict used purely as a cache for values.
    - etag_cache: a PersiDict used to cache ETag strings per key.

    For reads, the adapter consults etag_cache to decide whether the cached
    value is still valid. If the ETag hasn't changed in the main dict, the
    cached value is returned; otherwise the fresh value and ETag are fetched
    from main_dict and both caches are updated. All writes and deletions are
    performed against main_dict and mirrored into caches to keep them in sync.

    Notes:
      - main_dict must fully support ETag operations; caches must be mutable
        (append_only=False).
      - This class inherits type and serialization settings from main_dict.
    """

    def __init__(self,
                 main_dict: PersiDict[ValueType],
                 data_cache: PersiDict[ValueType],
                 etag_cache: PersiDict[str]) -> None:
        """Initialize with a main dict and two caches (data and ETag).

        Args:
            main_dict: The authoritative PersiDict that supports full ETag
                operations. All reads/writes/deletes are ultimately delegated
                here.
            data_cache: A mutable PersiDict used as a cache for values.
            etag_cache: A mutable PersiDict used to cache ETag strings.

        Raises:
            TypeError: If any of main_dict, data_cache, or etag_cache is not a
                PersiDict instance.
            ValueError: If either cache is append-only (append_only=True) or
                if main_dict does not fully support ETag operations.

        Notes:
            The adapter inherits base settings (base_class_for_values,
            serialization_format, and immutability) from main_dict to ensure compatibility.
        """

        inputs = dict(main_dict=main_dict
                      , data_cache=data_cache
                      , etag_cache=etag_cache)

        for k, v in inputs.items():
            if not isinstance(v, PersiDict):
                raise TypeError(f"{k} must be a PersiDict")
            if v.append_only:
                raise ValueError(f"{k} can't be append-only.")

        super().__init__(
            append_only=main_dict.append_only,
            base_class_for_values=main_dict.base_class_for_values,
            serialization_format=main_dict.serialization_format,
        )

        self._main_dict: PersiDict[ValueType] = main_dict
        self._data_cache: PersiDict[ValueType] = data_cache
        self._etag_cache: PersiDict[str] = etag_cache


    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Check membership against the main dict.

        Args:
            key: Non-empty key (tuple or coercible) to check.

        Returns:
            bool: True if the key exists in the main dict, False otherwise.
        """
        key = NonEmptySafeStrTuple(key)
        return key in self._main_dict

    def __len__(self) -> int:
        """Number of items in the main dict.

        Returns:
            int: Count of keys according to the main dict.
        """
        return len(self._main_dict)

    def _generic_iter(self, result_type: set[str]):
        """Delegate iteration to the main dict.

        Args:
            result_type: A set describing which items to iterate (implementation detail
                of PersiDict).

        Returns:
            Iterator over keys/values as provided by the main dict.
        """
        return self._main_dict._generic_iter(result_type)

    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Get the last-modified timestamp from the main dict.

        Args:
            key: Non-empty key to query.

        Returns:
            float: POSIX timestamp (seconds since epoch) as provided by the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        return self._main_dict.timestamp(key)

    def etag(self, key: NonEmptyPersiDictKey) -> str:
        """Return cached ETag if available, otherwise fetch from main dict.

        This method returns the ETag from the local cache when available,
        avoiding a (network) call to the main dict. If the ETag is not cached,
        it fetches from the main dict and caches the result.

        Note: The cached ETag may be stale if the value was modified directly
        in the main dict (bypassing this wrapper). However, reads via
        __getitem__ are self-healing and will detect/refresh stale caches.

        Args:
            key: Non-empty key to query.

        Returns:
            str: The ETag string for the key.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        cached_etag = self._etag_cache.get(key, None)
        if cached_etag is not None:
            return cached_etag
        # Not in cache - fetch from main_dict and cache it
        etag = self._main_dict.etag(key)
        self._set_cached_etag(key, etag)
        return etag


    def _set_cached_etag(self, key: NonEmptySafeStrTuple, etag: Optional[str]) -> None:
        """Update the cached ETag for a key, or clear it if None.

        Args:
            key: Normalized non-empty key.
            etag: The ETag string to store, or None to remove any cached ETag.
        """
        if etag is None:
            self._etag_cache.discard(key)
        else:
            self._etag_cache[key] = etag

    def __getitem__(self, key: NonEmptyPersiDictKey) -> ValueType:
        """Return the value for key using ETag-aware read-through caching.

        The method looks up the previously cached ETag for the key and asks the
        main dict if the item has changed. If not changed, it returns the value
        from the data cache; on a cache miss it fetches fresh data from the main
        dict, updates both caches, and returns the value.

        Args:
            key: Non-empty key to fetch.

        Returns:
            Any: The value associated with the key.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        old_etag = self._etag_cache.get(key, None)
        res = self.get_item_if_etag_changed(key, old_etag)
        if res is ETAG_HAS_NOT_CHANGED:
            try:
                return self._data_cache[key]
            except KeyError:
                value, _ =  self.get_item_if_etag_changed(key, None)
                return value
        else:
            value, _ = res
            return value


    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: Optional[str]):
        """Fetch value if the ETag is different from the provided one.

        Delegates to main_dict.get_item_if_new_etag. On change, updates both
        the data cache and the cached ETag. If the ETag has not changed, returns
        the ETAG_HAS_NOT_CHANGED sentinel.

        Args:
            key: Non-empty key to fetch.
            etag: Previously known ETag, or None to force fetching the value.

        Returns:
            tuple[Any, str] | ETAG_HAS_NOT_CHANGED: Either (value, new_etag) when
            the item is new or changed, or the ETAG_HAS_NOT_CHANGED sentinel when
            the supplied ETag matches the current one.
        """
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.get_item_if_etag_changed(key, etag)
        if res is ETAG_HAS_NOT_CHANGED:
            return res
        value, new_etag = res
        self._data_cache[key] = value
        self._set_cached_etag(key, new_etag)
        return res


    def get_item_if_etag_not_changed(self, key: NonEmptyPersiDictKey, etag: Optional[str]):
        """Return value only if the ETag matches the provided one.

        Validates against the main dict to avoid stale cached ETags. On success,
        refreshes both caches.
        """
        key = NonEmptySafeStrTuple(key)
        current_etag = self._main_dict.etag(key)
        if etag != current_etag:
            return ETAG_HAS_CHANGED

        try:
            value = self._data_cache[key]
        except KeyError:
            value = self._main_dict[key]
            self._data_cache[key] = value

        self._set_cached_etag(key, current_etag)
        return value, current_etag


    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType) -> None:
        """Set value for key via main dict and keep caches in sync.

        This method writes to the main dict and mirrors the value
        and ETag into caches.

        Args:
            key: Non-empty key to set.
            value: The value to store for the key.
        """
        # Reuse the base processing for jokers and type checks, but route actual
        # writes/deletes to the main dict and keep caches in sync via the
        # set_item_get_etag helper below.
        self.set_item_get_etag(key, value)


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: ValueType) -> Optional[str]:
        """Set item and return its ETag, updating caches.

        This method delegates the actual write to the main dict.
        After a successful write, it mirrors the value to data_cache
        and stores the returned ETag in etag_cache.

        Args:
            key: Non-empty key to set.
            value: The value to store.

        Returns:
            Optional[str]: The new ETag string from the main dict, or None if
            execution was handled entirely by base-class joker processing.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None
        etag = self._main_dict.set_item_get_etag(key, value)
        self._data_cache[key] = value
        self._set_cached_etag(key, etag)
        return etag


    def set_item_if_etag_not_changed(
            self,
            key: NonEmptyPersiDictKey,
            value: ValueType | Joker,
            etag: Optional[str]
    ) -> Optional[str] | ETagHasChangedFlag:
        """Set item only if ETag has not changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.set_item_if_etag_not_changed(key, value, etag)
        if res is ETAG_HAS_CHANGED:
            return res
        if value is KEEP_CURRENT:
            return res
        if value is DELETE_CURRENT:
            self._data_cache.discard(key)
            self._etag_cache.discard(key)
            return res
        self._data_cache[key] = value
        self._set_cached_etag(key, res)
        return res


    def set_item_if_etag_changed(
            self,
            key: NonEmptyPersiDictKey,
            value: ValueType | Joker,
            etag: Optional[str]
    ) -> Optional[str] | ETagHasNotChangedFlag:
        """Set item only if ETag has changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.set_item_if_etag_changed(key, value, etag)
        if res is ETAG_HAS_NOT_CHANGED:
            return res
        if value is KEEP_CURRENT:
            return res
        if value is DELETE_CURRENT:
            self._data_cache.discard(key)
            self._etag_cache.discard(key)
            return res
        self._data_cache[key] = value
        self._set_cached_etag(key, res)
        return res


    def delete_item_if_etag_not_changed(
            self,
            key: NonEmptyPersiDictKey,
            etag: Optional[str]
    ) -> None | ETagHasChangedFlag:
        """Delete item only if ETag has not changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.delete_item_if_etag_not_changed(key, etag)
        if res is ETAG_HAS_CHANGED:
            return res
        self._data_cache.discard(key)
        self._etag_cache.discard(key)
        return None


    def delete_item_if_etag_changed(
            self,
            key: NonEmptyPersiDictKey,
            etag: Optional[str]
    ) -> None | ETagHasNotChangedFlag:
        """Delete item only if ETag has changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.delete_item_if_etag_changed(key, etag)
        if res is ETAG_HAS_NOT_CHANGED:
            return res
        self._data_cache.discard(key)
        self._etag_cache.discard(key)
        return None


    def discard_item_if_etag_not_changed(
            self,
            key: NonEmptyPersiDictKey,
            etag: Optional[str]
    ) -> bool:
        """Discard item only if ETag has not changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.discard_item_if_etag_not_changed(key, etag)
        if res:
            self._data_cache.discard(key)
            self._etag_cache.discard(key)
        return res


    def discard_item_if_etag_changed(
            self,
            key: NonEmptyPersiDictKey,
            etag: Optional[str]
    ) -> bool:
        """Discard item only if ETag has changed; update caches on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.discard_item_if_etag_changed(key, etag)
        if res:
            self._data_cache.discard(key)
            self._etag_cache.discard(key)
        return res


    def __delitem__(self, key: NonEmptyPersiDictKey):
        """Delete key from main dict and purge caches if present.

        Deletion is delegated to the main dict using del.
        Cached value and ETag for the key (if any) are removed.

        Args:
            key: Non-empty key to delete.
            
        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        del self._main_dict[key]  # This will raise KeyError if key doesn't exist
        self._etag_cache.discard(key)
        self._data_cache.discard(key)

    def get_subdict(self, prefix_key: PersiDictKey) -> 'MutableDictCached[ValueType]':
        """Get a sub-dictionary for the given key prefix.

        Returns a new MutableDictCached with main_dict, data_cache, and
        etag_cache all scoped to the given prefix.

        Args:
            prefix_key: Prefix key (string or sequence of strings) identifying the
                subdictionary scope.

        Returns:
            MutableDictCached: A new cached dictionary rooted at the
                specified prefix.
        """
        prefix_key = SafeStrTuple(prefix_key)
        return MutableDictCached(
            main_dict=self._main_dict.get_subdict(prefix_key),
            data_cache=self._data_cache.get_subdict(prefix_key),
            etag_cache=self._etag_cache.get_subdict(prefix_key)
        )
