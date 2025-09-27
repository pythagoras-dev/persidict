from __future__ import annotations

from typing import Any, Optional

from .persi_dict import PersiDict, NonEmptyPersiDictKey
from .safe_str_tuple import NonEmptySafeStrTuple
from .singletons import ETAG_HAS_NOT_CHANGED, EXECUTION_IS_COMPLETE


class AppendOnlyDictCached(PersiDict):
    """Append-only dict facade with a read-through cache.

    This adapter wraps two concrete PersiDict instances:
    - main_dict: the source of truth that actually persists data.
    - data_cache: a second PersiDict used purely as a cache for values.

    Both the main dict and the cache must have append_only=True. Keys can
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
        compatible with main_dict's base_class_for_values and serialization_format.

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
            ValueError: If append_only is False for either dict, or the
                base_class_for_values between the two does not match.
        """
        if not isinstance(main_dict, PersiDict):
            raise TypeError("main_dict must be a PersiDict")
        if not isinstance(data_cache, PersiDict):
            raise TypeError("data_cache must be a PersiDict")
        if (not main_dict.append_only) or (not data_cache.append_only):
            raise ValueError("append_only must be set to True")
        if main_dict.base_class_for_values != data_cache.base_class_for_values:
            raise ValueError("main_dict and data_cache must have the same "
                             "base_class_for_values")

        # Initialize PersiDict base with parameters mirroring the main dict.
        super().__init__(
            append_only=True,
            base_class_for_values=main_dict.base_class_for_values,
            serialization_format=main_dict.serialization_format,
        )

        self._main: PersiDict = main_dict
        self._data_cache: PersiDict = data_cache


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
        # Items, added to the main_dict, are expected to never be removed.
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
                append_only is True.
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
                append_only is True.
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
