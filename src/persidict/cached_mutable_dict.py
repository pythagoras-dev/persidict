from __future__ import annotations

from typing import Any, Optional

from .persi_dict import PersiDict, NonEmptyPersiDictKey
from .safe_str_tuple import NonEmptySafeStrTuple
from .singletons import ETAG_HAS_NOT_CHANGED, EXECUTION_IS_COMPLETE


class MutableDictCached(PersiDict):
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
                 main_dict: PersiDict,
                 data_cache: PersiDict,
                 etag_cache: PersiDict) -> None:
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

        self._main_dict = main_dict
        self._data_cache = data_cache
        self._etag_cache = etag_cache


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

    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
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


    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any):
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


    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> Optional[str]:
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


