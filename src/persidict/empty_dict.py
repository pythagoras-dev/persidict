"""EmptyDict: EmptyDict implementation that discards writes, always appears empty.

This module provides EmptyDict, a persistent dictionary that behaves like
/dev/null - accepting all writes but discarding them, and always appearing
empty on reads. Useful for testing, debugging, or as a no-op placeholder.
"""
from __future__ import annotations

from typing import Any, Iterator

from .safe_str_tuple import NonEmptySafeStrTuple
from .persi_dict import PersiDict, PersiDictKey, NonEmptyPersiDictKey, ValueType
from .jokers_and_status_flags import (ETagConditionFlag, ETagIfExists,
                                      Joker,
                                      ITEM_NOT_AVAILABLE,
                                      ConditionalOperationResult,
                                      OperationResult)


class EmptyDict(PersiDict[ValueType]):
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


    def __getitem__(self, key: NonEmptyPersiDictKey) -> ValueType:
        """Always raises KeyError as EmptyDict contains nothing."""
        raise KeyError(key)


    def get_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Key is always absent; condition evaluated with actual_etag=ITEM_NOT_AVAILABLE."""
        NonEmptySafeStrTuple(key)
        satisfied = self._check_condition(condition, expected_etag, ITEM_NOT_AVAILABLE)
        return self._result_item_not_available(condition, satisfied)


    def __setitem__(self, key: NonEmptyPersiDictKey, value: ValueType) -> None:
        """Accepts any write operation, discards the data (like /dev/null)."""
        # Run base validations (immutable checks, key normalization,
        # type checks, jokers) to ensure API consistency, then discard.
        self._process_setitem_args(key, value)
        # Do nothing - discard the write like /dev/null


    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            value: ValueType | Joker,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Key is always absent; condition evaluated, write discarded on success."""
        self._validate_setitem_args(key, value)
        satisfied = self._check_condition(condition, expected_etag, ITEM_NOT_AVAILABLE)
        return self._result_item_not_available(condition, satisfied)

    def setdefault_if(
            self,
            key: NonEmptyPersiDictKey,
            default_value: ValueType,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Key is always absent; condition evaluated, write discarded on success."""
        if isinstance(default_value, Joker):
            raise TypeError("default_value must be a regular value, not a Joker command")
        NonEmptySafeStrTuple(key)
        satisfied = self._check_condition(condition, expected_etag, ITEM_NOT_AVAILABLE)
        return self._result_item_not_available(condition, satisfied)

    def discard_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag
    ) -> ConditionalOperationResult:
        """Key is always absent; condition evaluated normally."""
        NonEmptySafeStrTuple(key)
        satisfied = self._check_condition(condition, expected_etag, ITEM_NOT_AVAILABLE)
        return self._result_item_not_available(condition, satisfied)

    def transform_item(self, key, transformer, *, n_retries: int | None = 6) -> OperationResult:
        """Transform always receives ITEM_NOT_AVAILABLE, result is discarded."""
        return OperationResult(
            resulting_etag=ITEM_NOT_AVAILABLE,
            new_value=ITEM_NOT_AVAILABLE)

    
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


    def get(self, key: NonEmptyPersiDictKey, default: ValueType | None = None) -> ValueType | None:
        """Always returns the default value since key is never found."""
        return default


    def setdefault(self, key: NonEmptyPersiDictKey, default: ValueType | None = None) -> ValueType | None:
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


    def get_subdict(self, prefix_key: PersiDictKey) -> 'EmptyDict[ValueType]':
        """Returns a new EmptyDict as subdictionary.

        Args:
            prefix_key: Key prefix (ignored, as EmptyDict has no hierarchical structure).

        Returns:
            EmptyDict: A new EmptyDict instance with the same configuration.
        """
        return EmptyDict(**self.get_params())
