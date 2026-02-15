"""Read-through cached, append-only persistent dictionary adapter.

This module provides `AppendOnlyDictCached`, an append-only facade that
combines two concrete `PersiDict` implementations:

- `main_dict`: the authoritative store (source of truth) where data is
  actually persisted;
- `data_cache`: a secondary `PersiDict` that is used strictly as a cache for
  values.

Because both backends are append-only (items may be added once and never
modified or deleted), the cache can be trusted once it has a value for a key.
Reads go to the cache first and fall back to the main dict on a miss, at which
point the cache is populated. Writes always go to the main dict first and are
mirrored to the cache after validation performed by the `PersiDict` base.

The adapter delegates iteration, length, timestamps, and base properties to the
main dict to keep semantics consistent with the authoritative store.
"""

from __future__ import annotations


from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey, ValueType
from .safe_str_tuple import NonEmptySafeStrTuple, SafeStrTuple
from .jokers_and_status_flags import (EXECUTION_IS_COMPLETE,
                                      Joker,
                                      ETagValue,
                                      ETagConditionFlag,
                                      ANY_ETAG,
                                      ETagIfExists,
                                      RetrieveValueFlag, IF_ETAG_CHANGED,
                                      NEVER_RETRIEVE,
                                      ITEM_NOT_AVAILABLE, VALUE_NOT_RETRIEVED,
                                      ConditionalOperationResult,
                                      OperationResult,
                                      TransformingFunction)


class AppendOnlyDictCached(PersiDict[ValueType]):
    """Append-only `PersiDict` facade with a read-through cache.

    This adapter composes two concrete `PersiDict` instances and presents them
    as a single append-only mapping. It trusts the cache because both backends
    are append-only: once a key is written it will never be modified or
    deleted.

    Behavior summary:

    - Reads: `__getitem__` first tries the cache, falls back to the main dict,
      then populates the cache on a miss.
    - Membership: `__contains__` returns True immediately if the key is in the
      cache; otherwise it checks the main dict.
    - Writes: `__setitem__` writes to the main dict and then mirrors the value
      into the cache (after base validation performed by `PersiDict`).
    - `set_item_if`: delegates the write to the main dict, mirrors the
      value into the cache on success.
    - Deletion: not supported (append-only), will raise `TypeError`.
    - Iteration/length/timestamps: delegated to the main dict.

    Attributes:
      _main: The authoritative append-only `PersiDict` instance.
      _data_cache: The append-only `PersiDict` used purely as a value cache.

    Args:
      main_dict: The authoritative append-only `PersiDict`.
      data_cache: A `PersiDict` used as a cache; must be append-only and
        compatible with `main_dict` (same `base_class_for_values` and
        `serialization_format`).

    Raises:
      TypeError: If `main_dict` or `data_cache` are not `PersiDict` instances.
      ValueError: If either dict is not append-only or their
        `base_class_for_values` differ.

    """

    def __init__(self, *,
                 main_dict: PersiDict[ValueType],
                 data_cache: PersiDict[ValueType]) -> None:
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

        self._main: PersiDict[ValueType] = main_dict
        self._data_cache: PersiDict[ValueType] = data_cache


    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Check whether a key exists in the cache or main dict.

        The cache is checked first and trusted because both dicts are
        append-only. On a cache miss, the main dict is consulted.

        Args:
            key: Dictionary key (string or sequence of strings or
                NonEmptySafeStrTuple).

        Returns:
            True if the key exists.
        """
        key = NonEmptySafeStrTuple(key)
        if key in self._data_cache:
        # Items, added to the main_dict, are expected to never be removed.
        # Hence, it's OK to trust the cache without verifying the main dict
            return True
        else:
            return key in self._main

    def __len__(self) -> int:
        """Return the number of items.

        Returns:
            Number of items in the dictionary, delegated to the main dict.
        """
        return len(self._main)

    def _generic_iter(self, result_type: set[str]):
        """Internal iterator dispatcher delegated to the main dict.

        Args:
            result_type: Non-empty subset of {"keys", "values",
                "timestamps"} specifying which fields to yield.

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
            POSIX timestamp of the last write for the key.

        Raises:
            KeyError: If the key does not exist in the main dict.
        """
        key = NonEmptySafeStrTuple(key)
        return self._main.timestamp(key)

    def etag(self, key: NonEmptyPersiDictKey) -> ETagValue:
        """Return the ETag from the main dict.

        Delegating to the main dict preserves backend-specific ETag semantics
        (e.g., native S3 ETags) instead of deriving ETags from timestamps.
        """
        key = NonEmptySafeStrTuple(key)
        return self._main.etag(key)



    def __getitem__(self, key: NonEmptyPersiDictKey) -> ValueType:
        """Retrieve a value using a read-through cache.

        Tries the cache first; on a miss, reads from the main dict, stores the
        value into the cache, and returns it.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.

        Returns:
            The stored value.

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


    def get_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Return value only if its ETag satisfies a condition; cache on success."""
        key = NonEmptySafeStrTuple(key)
        res = self._main.get_item_if(
            key, condition=condition, expected_etag=expected_etag,
            retrieve_value=retrieve_value)
        # Cache the value if it was retrieved and not already cached
        if (res.new_value is not ITEM_NOT_AVAILABLE
                and res.new_value is not VALUE_NOT_RETRIEVED
                and key not in self._data_cache):
            self._data_cache[key] = res.new_value
        return res

    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType | Joker) -> None:
        """Store a value in the main dict and mirror it into the cache.

        Uses ``setdefault_if`` for insert-if-absent on the main dict
        (atomic when the main dict supports conditional writes), then
        mirrors the value into the cache.

        Args:
            key: Dictionary key (string or sequence of strings) or
                NonEmptySafeStrTuple.
            value: The value to store, or a joker (KEEP_CURRENT/DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item (append-only).
            TypeError: If the value fails base_class_for_values validation.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return
        result = self.setdefault_if(
            key,
            default_value=value,
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=NEVER_RETRIEVE,
        )
        if not result.value_was_mutated:
            raise KeyError("Can't modify an immutable key-value pair")

    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            value: ValueType | Joker,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Append-only: delegates to main dict; caches a returned value when available."""
        key = NonEmptySafeStrTuple(key)
        res = self._main.set_item_if(
            key, value=value, condition=condition, expected_etag=expected_etag,
            retrieve_value=retrieve_value)
        if (res.new_value is not ITEM_NOT_AVAILABLE
                and res.new_value is not VALUE_NOT_RETRIEVED
                and not isinstance(res.new_value, Joker)):
            if key not in self._data_cache:
                self._data_cache[key] = res.new_value
        return res

    def setdefault_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            default_value: ValueType,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Insert default if absent and condition satisfied; delegate to main dict."""
        key = NonEmptySafeStrTuple(key)
        res = self._main.setdefault_if(
            key, default_value=default_value, condition=condition,
            expected_etag=expected_etag, retrieve_value=retrieve_value)
        if (res.new_value is not ITEM_NOT_AVAILABLE
                and res.new_value is not VALUE_NOT_RETRIEVED
                and key not in self._data_cache):
            self._data_cache[key] = res.new_value
        return res

    def discard_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists
    ) -> ConditionalOperationResult[ValueType]:
        """Deletion is not supported for append-only dictionaries."""
        raise TypeError("append-only dicts do not support deletion")

    def transform_item(
            self,
            key: NonEmptyPersiDictKey,
            *,
            transformer: TransformingFunction[ValueType],
            n_retries: int | None = 6
    ) -> OperationResult[ValueType]:
        """Not supported for append-only dictionaries."""
        raise NotImplementedError("append-only dicts do not support transform_item")

    def __delitem__(self, key: NonEmptyPersiDictKey):
        """Deletion is not supported for append-only dictionaries.

        Raises:
            TypeError: Always raised to indicate append-only restriction.
        """
        raise TypeError("append-only dicts do not support deletion")

    def get_subdict(self, prefix_key: PersiDictKey) -> 'AppendOnlyDictCached[ValueType]':
        """Get a sub-dictionary for the given key prefix.

        Returns a new AppendOnlyDictCached with main_dict and data_cache
        both scoped to the given prefix.

        Args:
            prefix_key: Prefix key (string or sequence of strings) identifying the
                subdictionary scope.

        Returns:
            A new cached dictionary rooted at the
                specified prefix.
        """
        prefix_key = SafeStrTuple(prefix_key)
        return AppendOnlyDictCached(
            main_dict=self._main.get_subdict(prefix_key),
            data_cache=self._data_cache.get_subdict(prefix_key)
        )
