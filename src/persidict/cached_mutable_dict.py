"""Mutable persistent dictionary adapter with ETag-aware read-through caching.

This module provides `MutableDictCached`, an adapter that composes three
concrete `PersiDict` instances (main dict, data cache, ETag cache) to
present a single mutable mapping with local caching. ETag validation
ensures cached values are consistent with the authoritative store.
"""

from __future__ import annotations


from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey, ValueType
from .safe_str_tuple import NonEmptySafeStrTuple, SafeStrTuple
from .jokers_and_status_flags import (EXECUTION_IS_COMPLETE,
                                      Joker,
                                      ETagValue,
                                      ETagConditionFlag,
                                      ETAG_HAS_CHANGED,
                                      RetrieveValueFlag, ALWAYS_RETRIEVE,
                                      IF_ETAG_CHANGED,
                                      ITEM_NOT_AVAILABLE, ItemNotAvailableFlag,
                                      ValueNotRetrievedFlag,
                                      VALUE_NOT_RETRIEVED,
                                      ETagIfExists,
                                      ConditionalOperationResult,
                                      OperationResult,
                                      TransformingFunction)


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
                 etag_cache: PersiDict[ETagValue]) -> None:
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
            ValueError: If any of main_dict, data_cache, or etag_cache is
                append-only (append_only=True).

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
        self._etag_cache: PersiDict[ETagValue] = etag_cache


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

    def etag(self, key: NonEmptyPersiDictKey) -> ETagValue:
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
            ETagValue: The ETag string for the key.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        try:
            cached_etag = self._etag_cache[key]
            return cached_etag
        except KeyError:
            pass
        # Not in cache - fetch from main_dict and cache it
        etag = self._main_dict.etag(key)
        self._set_cached_etag(key, etag)
        return etag


    def _set_cached_etag(self, key: NonEmptySafeStrTuple, etag: ETagValue) -> None:
        """Update the cached ETag for a key.

        Args:
            key: Normalized non-empty key.
            etag: The ETag string to store.
        """
        self._etag_cache[key] = etag

    def _purge_caches(self, key: NonEmptySafeStrTuple) -> None:
        """Remove any cached value/etag for a key."""
        self._data_cache.discard(key)
        self._etag_cache.discard(key)

    def _sync_caches_from_result(
            self,
            key: NonEmptySafeStrTuple,
            *,
            new_value: ValueType | ItemNotAvailableFlag | ValueNotRetrievedFlag,
            resulting_etag: ETagIfExists,
            actual_etag: ETagIfExists | None = None
    ) -> None:
        """Update or clear caches based on an operation result.

        Args:
            key: The key that was operated on.
            new_value: The value returned by the operation, or a sentinel.
            resulting_etag: The ETag after the operation completed.
            actual_etag: The ETag observed *before* the operation (from
                ``ConditionalOperationResult.actual_etag``).  When provided
                alongside ``VALUE_NOT_RETRIEVED``, it lets us distinguish
                a successful mutation (``actual_etag != resulting_etag``)
                from a no-op (same etags).
        """
        if new_value is ITEM_NOT_AVAILABLE or isinstance(resulting_etag, ItemNotAvailableFlag):
            self._purge_caches(key)
            return
        if new_value is VALUE_NOT_RETRIEVED:
            # Value wasn't fetched.  If we can prove the item was mutated
            # (actual_etag differs from resulting_etag), purge the stale
            # cached value and record the new etag so etag() stays
            # accurate.  When the etags match the item is unchanged and
            # caches (even if stale from some other cause) are left alone
            # — the self-healing __getitem__ path will fix them on read.
            if actual_etag is not None and actual_etag != resulting_etag:
                self._data_cache.discard(key)
                if not isinstance(resulting_etag, ItemNotAvailableFlag):
                    self._set_cached_etag(key, resulting_etag)
            return
        self._data_cache[key] = new_value
        if not isinstance(resulting_etag, ItemNotAvailableFlag):
            self._set_cached_etag(key, resulting_etag)

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

        # Check if we have a cached etag
        try:
            cached_etag = self._etag_cache[key]
        except KeyError:
            cached_etag = ITEM_NOT_AVAILABLE

        # If etag is cached but data is not, skip the conditional GET
        # and fetch directly — avoids a wasted IfNoneMatch round-trip.
        has_cached_data = key in self._data_cache
        if cached_etag is not ITEM_NOT_AVAILABLE and not has_cached_data:
            res = self.get_item_if(
                key, ITEM_NOT_AVAILABLE, ETAG_HAS_CHANGED)
            if res.new_value is ITEM_NOT_AVAILABLE:
                raise KeyError(f"Key {key} not found")
            return res.new_value

        res = self.get_item_if(
            key, cached_etag, ETAG_HAS_CHANGED,
            retrieve_value=IF_ETAG_CHANGED)

        if res.new_value is ITEM_NOT_AVAILABLE:
            raise KeyError(f"Key {key} not found")

        if res.new_value is VALUE_NOT_RETRIEVED:
            # Etag hasn't changed, return from data cache
            try:
                return self._data_cache[key]
            except KeyError:
                # Shouldn't happen (we checked above), but handle gracefully
                res2 = self.get_item_if(
                    key, ITEM_NOT_AVAILABLE, ETAG_HAS_CHANGED)
                if res2.new_value is ITEM_NOT_AVAILABLE:
                    raise KeyError(f"Key {key} not found")
                return res2.new_value

        return res.new_value

    def get_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            retrieve_value: RetrieveValueFlag = ALWAYS_RETRIEVE
    ) -> ConditionalOperationResult:
        """Return value only if the ETag satisfies a condition.

        Delegates to the main dict and refreshes caches when data is fetched.
        """
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.get_item_if(
            key, expected_etag, condition,
            retrieve_value=retrieve_value)
        self._sync_caches_from_result(
            key, new_value=res.new_value, resulting_etag=res.resulting_etag,
            actual_etag=res.actual_etag)
        return res


    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType) -> None:
        """Set value for key via main dict and keep caches in sync.

        Args:
            key: Non-empty key to set.
            value: The value to store for the key.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return
        self._main_dict[key] = value
        self._data_cache[key] = value
        etag = self._main_dict.etag(key)
        self._set_cached_etag(key, etag)


    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            value: ValueType | Joker,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            retrieve_value: RetrieveValueFlag = ALWAYS_RETRIEVE
    ) -> ConditionalOperationResult:
        """Set item only if ETag satisfies a condition; update caches when a value is returned."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.set_item_if(
            key, value, expected_etag, condition,
            retrieve_value=retrieve_value)
        self._sync_caches_from_result(
            key, new_value=res.new_value, resulting_etag=res.resulting_etag,
            actual_etag=res.actual_etag)
        return res


    def setdefault_if(
            self,
            key: NonEmptyPersiDictKey,
            default_value: ValueType,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            retrieve_value: RetrieveValueFlag = ALWAYS_RETRIEVE
    ) -> ConditionalOperationResult:
        """Insert default if absent and condition satisfied; delegate to main dict."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.setdefault_if(
            key, default_value, expected_etag, condition,
            retrieve_value=retrieve_value)
        self._sync_caches_from_result(
            key, new_value=res.new_value, resulting_etag=res.resulting_etag,
            actual_etag=res.actual_etag)
        return res

    def discard_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag
    ) -> ConditionalOperationResult:
        """Discard item only if ETag satisfies a condition; update caches."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.discard_item_if(key, expected_etag, condition)
        self._sync_caches_from_result(
            key, new_value=res.new_value, resulting_etag=res.resulting_etag,
            actual_etag=res.actual_etag)
        return res

    def transform_item(
            self,
            key: NonEmptyPersiDictKey,
            transformer: TransformingFunction,
            *,
            n_retries: int | None = 6
    ) -> OperationResult:
        """Apply a transformation; delegate to main dict and update caches."""
        key = NonEmptySafeStrTuple(key)
        res = self._main_dict.transform_item(key, transformer, n_retries=n_retries)
        self._sync_caches_from_result(
            key, new_value=res.new_value, resulting_etag=res.resulting_etag)
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
        self._sync_caches_from_result(
            key, new_value=ITEM_NOT_AVAILABLE, resulting_etag=ITEM_NOT_AVAILABLE)

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
