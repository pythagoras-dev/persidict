from __future__ import annotations

from typing import Any, Optional

from .persi_dict import PersiDict, NonEmptyPersiDictKey
from .safe_str_tuple import NonEmptySafeStrTuple
from .singletons import ETAG_HAS_NOT_CHANGED, EXECUTION_IS_COMPLETE


class ETaggableDictCached(PersiDict):
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
        (immutable_items=False).
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
            ValueError: If either cache is append-only (immutable_items=True) or
                if main_dict does not fully support ETag operations.

        Notes:
            The adapter inherits base settings (digest_len, base_class_for_values,
            file_type, and immutability) from main_dict to ensure compatibility.
        """

        inputs = dict(main_dict=main_dict
                      , data_cache=data_cache
                      , etag_cache=etag_cache)

        for k, v in inputs.items():
            if not isinstance(v, PersiDict):
                raise TypeError(f"{k} must be a PersiDict")
            if v.immutable_items:
                raise ValueError(f"{k} can't be append-only "
                                 "(immutable_items must be False)")
        if not main_dict.native_etags:
            raise ValueError("main_dict must fully support etags")

        super().__init__(
            immutable_items=main_dict.immutable_items,
            digest_len=main_dict.digest_len,
            base_class_for_values=main_dict.base_class_for_values,
            file_type=main_dict.file_type,
        )

        self._main_dict = main_dict
        self._data_cache = data_cache
        self._etag_cache = etag_cache


    @property
    def prefix_key(self):
        """tuple[str, ...]: The same prefix as the main dict's namespace.

        Returns:
            tuple[str, ...]: The logical prefix used for all keys, delegated to
            the main dict to ensure both share the same namespace.
        """
        # Keep the same logical namespace as the main dict
        return self._main_dict.prefix_key


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


    @property
    def native_etags(self) -> bool:
        """Whether ETag operations are natively supported by a dictionary class.

        False by default, means the timestamp is used in lieu of ETag.
        True means the class provides custom ETag implementation.
        """
        return self._main_dict.native_etags


    def _set_cached_etag(self, key: NonEmptySafeStrTuple, etag: Optional[str]) -> None:
        """Update the cached ETag for a key, or clear it if None.

        Args:
            key: Normalized non-empty key.
            etag: The ETag string to store, or None to remove any cached ETag.
        """
        if etag is None:
            self._etag_cache.delete_if_exists(key)
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

        Deletion is delegated to the main dict using delete_if_exists.
        Cached value and ETag for the key (if any) are removed.

        Args:
            key: Non-empty key to delete.
        """
        key = NonEmptySafeStrTuple(key)
        self._main_dict.delete_if_exists(key)
        self._etag_cache.delete_if_exists(key)
        self._data_cache.delete_if_exists(key)


class AppendOnlyDictCached(PersiDict):
    """Append-only dict facade with a read-through cache.

    This adapter wraps two concrete PersiDict instances:
    - main_dict: the source of truth that actually persists data.
    - data_cache: a second PersiDict used purely as a cache for values.

    Both the main dict and the cache must have immutable_items=True. Keys can
    be added once but never modified or deleted. Because of that contract, the
    cache can be trusted when it already has a value for a key without
    re-validating the main dict.

    Behavior summary:
    - Reads: __getitem__ first tries the cache, falls back to the main dict and
      populates the cache on a miss.
    - Membership: __contains__ returns True if the key is in the cache; else it
      checks the main dict.
    - Writes: __setitem__ writes to the main dict and mirrors the value into
      the cache after argument validation by the PersiDict base.
    - set_item_get_etag delegates the write to the main dict, mirrors the value
      into the cache, and returns the ETag from the main dict.
    - Deletion is not supported and will raise TypeError (append-only).
    - Iteration, length, timestamps, base_url and base_dir are delegated to the
      main dict. get_item_if_new_etag is delegated too, and on change the
      value is cached.

    Args:
      main_dict: The authoritative append-only PersiDict.
      data_cache: A PersiDict used as a value cache; must be append-only and
        compatible with main_dict's base_class_for_values and file_type.

    Raises:
      TypeError: If main_dict or data_cache are not PersiDict instances.
      ValueError: If either dict is not immutable (append-only) or their
        base_class_for_values differ.
    """

    def __init__(self,
                 main_dict: PersiDict,
                 data_cache: PersiDict) -> None:
        """Initialize the adapter with a main dict and a value cache.

        Args:
            main_dict: The authoritative append-only PersiDict instance.
            data_cache: A PersiDict used as a read-through cache for values.

        Raises:
            TypeError: If main_dict or data_cache are not PersiDict instances.
            ValueError: If immutable_items is False for either dict, or the
                base_class_for_values between the two does not match.
        """
        if not isinstance(main_dict, PersiDict):
            raise TypeError("main_dict must be a PersiDict")
        if not isinstance(data_cache, PersiDict):
            raise TypeError("data_cache must be a PersiDict")
        if (not main_dict.immutable_items) or (not data_cache.immutable_items):
            raise ValueError("immutable_items must be set to True")
        if main_dict.base_class_for_values != data_cache.base_class_for_values:
            raise ValueError("main_dict and data_cache must have the same "
                             "base_class_for_values")

        # Initialize PersiDict base with parameters mirroring the main dict.
        super().__init__(
            immutable_items=True,
            digest_len=main_dict.digest_len,
            base_class_for_values=main_dict.base_class_for_values,
            file_type=main_dict.file_type,
        )

        self._main: PersiDict = main_dict
        self._data_cache: PersiDict = data_cache

    @property
    def prefix_key(self):
        """tuple[str, ...]: Prefix of keys identical to the main dict's.

        Delegates to main_dict.prefix_key to keep the same logical namespace.
        """
        # Keep the same logical namespace as the main dict
        return self._main.prefix_key

    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Check whether a key exists in the cache or main dict.

        The cache is checked first and trusted because both dicts are
        append-only. On a cache miss, the main dict is consulted.

        Args:
            key: Dictionary key (string or sequence of strings or
                NonEmptySafeStrTuple).

        Returns:
            bool: True if the key exists.
        """
        key = NonEmptySafeStrTuple(key)
        if key in self._data_cache:
        # Items, added to the main_dict, are expected to never be removed
        # Hence, it's OK to trust the cache without verifying the main dict
            return True
        else:
            return key in self._main

    def __len__(self) -> int:
        """int: Number of items, delegated to the main dict."""
        return len(self._main)

    def _generic_iter(self, result_type: set[str]):
        """Internal iterator dispatcher delegated to the main dict.

        Args:
            result_type: A set describing what to iterate, as used by
                PersiDict internals (e.g., {"keys"}, {"items"}, etc.).

        Returns:
            An iterator over the requested view, produced by the main dict.
        """
        return self._main._generic_iter(result_type)

    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Return item's timestamp from the main dict.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.

        Returns:
            float: POSIX timestamp of the last write for the key.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        return self._main.timestamp(key)


    @property
    def native_etags(self) -> bool:
        """Whether ETag operations are natively supported by a dictionary class.

        False by default, means the timestamp is used in lieu of ETag.
        True means the class provides custom ETag implementation.
        """
        return self._main.native_etags


    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        """Retrieve a value using a read-through cache.

        Tries the cache first; on a miss, reads from the main dict, stores the
        value into the cache, and returns it.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.

        Returns:
            Any: The stored value.

        Raises:
            KeyError: If the key is missing in the main dict (and therefore
                also not present in the cache).
        """
        key = NonEmptySafeStrTuple(key)
        try:
            # Items, added to the main_dict, are expected to never be removed
            # Hence, it's OK to trust the cache without verifying the main dict
            return self._data_cache[key]
        except KeyError:
            value = self._main[key]
            self._data_cache[key] = value
            return value


    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: Optional[str]):
        """Return value only if its ETag changed; cache the value if so.

        Delegates to the main dict. If the ETag differs from the provided one,
        the new value is cached and the (value, etag) tuple is returned.
        Otherwise, returns ETAG_HAS_NOT_CHANGED.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.
            etag: Previously seen ETag or None.

        Returns:
            tuple[Any, str|None] | ETagHasNotChangedFlag: The value and the new
            ETag when changed; ETAG_HAS_NOT_CHANGED otherwise.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        res = self._main.get_item_if_etag_changed(key, etag)
        if not res is ETAG_HAS_NOT_CHANGED:
            value, _ = res
            self._data_cache[key] = value
        return res

    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any):
        """Store a value in the main dict and mirror it into the cache.

        The PersiDict base validates special joker values and the
        base_class_for_values via _process_setitem_args. On successful
        validation, the value is written to the main dict and then cached.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.
            value: The value to store, or a joker (KEEP_CURRENT/DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If the value fails base_class_for_values validation.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return
        self._main[key] = value
        self._data_cache[key] = value

    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any) -> Optional[str]:
        """Store a value and return the ETag from the main dict.

        After validation via _process_setitem_args, the value is written to the
        main dict using its ETag-aware API, then mirrored into the cache.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.
            value: The value to store, or a joker (KEEP_CURRENT/DELETE_CURRENT).

        Returns:
            str | None: The ETag produced by the main dict, or None if a joker
            short-circuited the operation or the backend doesn't support ETags.

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If the value fails base_class_for_values validation.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None
        etag = self._main.set_item_get_etag(key, value)
        self._data_cache[key] = value
        return etag

    def __delitem__(self, key: NonEmptyPersiDictKey):
        """Deletion is not supported for append-only dictionaries.

        Raises:
            TypeError: Always raised to indicate append-only restriction.
        """
        raise TypeError("append-only dicts do not support deletion")
