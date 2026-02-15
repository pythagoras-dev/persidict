"""Write-once dictionary with probabilistic consistency checking.

This module provides WriteOnceDict, a wrapper around PersiDict that supports
an alternative behavior of append-only dictionaries.
It allows repeated writes to an existing key but assumes that
all the subsequent writes have exactly the same value as the first one,
so they can be safely ignored. Random consistency checks ensure that
repeated writes contain the same values, helping detect data consistency issues.
Setting the probability of random checks to 0 disables them.
"""
from __future__ import annotations

from deepdiff import DeepDiff
from mixinforge import sort_dict_by_keys

from .jokers_and_status_flags import (
    KEEP_CURRENT,
    KeepCurrentFlag,
    Joker,
    EXECUTION_IS_COMPLETE,
    ETagConditionFlag,
    ETagIfExists,
    RetrieveValueFlag, IF_ETAG_CHANGED,
    ConditionalOperationResult,
    ANY_ETAG,
    ITEM_NOT_AVAILABLE,
    ALWAYS_RETRIEVE,
    NEVER_RETRIEVE,
)
from .persi_dict import PersiDict, NonEmptyPersiDictKey, ValueType
from .safe_str_tuple import NonEmptySafeStrTuple
from .file_dir_dict import FileDirDict
import random
import sys
from typing import Any

import joblib.hashing

def _get_md5_signature(x: Any) -> str:
    """Compute an MD5 signature for an arbitrary Python object.

    Uses joblib's Hasher (or NumpyHasher when NumPy is available). joblib
    relies on Pickle for serialization, except for NumPy arrays which are
    handled by optimized routines. The resulting digest is returned as a
    lower-case base16 string.

    Args:
        x: Any serializable Python object. NumPy arrays are supported with
            specialized hashing.

    Returns:
        The base16 MD5 hash of the object.
    """
    if 'numpy' in sys.modules:
        hasher = joblib.hashing.NumpyHasher(hash_name='md5')
    else:
        hasher = joblib.hashing.Hasher(hash_name='md5')
    hash_signature = hasher.hash(x)
    return str(hash_signature)

class WriteOnceDict(PersiDict[ValueType]):
    """Dictionary wrapper that preserves the first value written for each key.

    Subsequent writes to an existing key are allowed but ignored as they are
    expected to have exactly the same value. They are randomly checked
    against the original value to ensure consistency. If a randomly triggered
    check finds a difference, a ValueError is raised. The probability of
    performing a check is controlled by ``p_consistency_checks``.

    This is useful in concurrent or distributed settings where the same value
    is assumed to be assigned repeatedly to the same key,
    and you want to check this assumption (detect divergent values)
    without paying the full cost of always comparing values.

    **API limitation:** ``set_item_if`` is not supported and raises
    ``NotImplementedError``. Conditional overwrites contradict write-once
    semantics. Insert-if-absent is available via ``setdefault_if`` on the
    wrapped dict (and is used internally by ``__setitem__``).

    **Atomicity note:** insert-if-absent semantics in ``__setitem__`` are
    delegated to the wrapped backend's ``setdefault_if``. Atomicity is only
    guaranteed when the wrapped backend provides it (e.g. ``BasicS3Dict``
    uses S3 conditional headers). The default ``PersiDict`` base
    implementation is *not* atomic.
    """
    _wrapped_dict: PersiDict[ValueType]
    _p_consistency_checks: float | None
    _consistency_checks_attempted: int
    _consistency_checks_passed: int

    def __init__(self, *,
                 wrapped_dict: PersiDict[ValueType] | None = None,
                 p_consistency_checks: float | None = None):
        """Initialize a WriteOnceDict.

        Args:
            wrapped_dict: The underlying persistent dictionary to wrap. If not
                provided, a FileDirDict with append_only=True is created.
            p_consistency_checks: Probability in [0, 1] to perform a
                consistency check when a key already exists. ``None`` means 0.0
                (disabled).

        Raises:
            TypeError: If ``wrapped_dict`` is not a PersiDict instance.
            ValueError: If ``wrapped_dict`` does not enforce immutable items.
        """
        if wrapped_dict is None:
            wrapped_dict = FileDirDict(append_only=True)
        if not isinstance(wrapped_dict, PersiDict):
            raise TypeError("wrapped_dict must be a PersiDict instance")
        if wrapped_dict.append_only is not True:
            raise ValueError("wrapped_dict must be append-only")
        self.p_consistency_checks = p_consistency_checks
        PersiDict.__init__(self,
            base_class_for_values=wrapped_dict.base_class_for_values,
            serialization_format=wrapped_dict.serialization_format,
            append_only=True)
        self._wrapped_dict = wrapped_dict
        self._consistency_checks_passed = 0
        self._consistency_checks_attempted = 0


    @property
    def p_consistency_checks(self) -> float:
        """Probability of checking a new value against the first value stored.

        Returns:
            Probability in [0, 1].
        """
        return self._p_consistency_checks


    @p_consistency_checks.setter
    def p_consistency_checks(self, value: float | None | KeepCurrentFlag) -> None:
        """Set the probability of performing consistency checks.

        Args:
            value: Probability in [0, 1]. ``None`` is treated as 0.0.
                ``KEEP_CURRENT`` can be used only after initialization to
                preserve the current value.

        Raises:
            ValueError: If used with ``KEEP_CURRENT`` during initialization or
                if ``value`` is outside [0, 1].
        """
        if value is KEEP_CURRENT:
            if hasattr(self, '_p_consistency_checks'):
                return
            else:
                raise ValueError(
                    "KEEP_CURRENT can't be used to initialize p_consistency_checks.")
        if value is None:
            value = 0.0
        if not (0 <= value <= 1):
            raise ValueError(
                f"p_consistency_checks must be in [0, 1], "
                f"got {value}.")
        self._p_consistency_checks = value


    @property
    def consistency_checks_failed(self) -> int:
        """Number of failed consistency checks.

        Returns:
            Failed checks (attempted - passed).
        """
        return (self._consistency_checks_attempted
                - self._consistency_checks_passed)


    @property
    def consistency_checks_attempted(self) -> int:
        """Number of attempted consistency checks.

        Returns:
            Attempted checks counter.
        """
        return self._consistency_checks_attempted


    @property
    def consistency_checks_passed(self) -> int:
        """Number of successful consistency checks.

        Returns:
            Passed checks counter.
        """
        return self._consistency_checks_passed


    def get_params(self) -> dict[str, Any]:
        """Return parameterization of this instance.

        Returns:
            A dictionary with keys 'wrapped_dict' and
            'p_consistency_checks', sorted by keys for deterministic
            comparison/serialization.
        """
        params = dict(
            wrapped_dict=self._wrapped_dict,
            p_consistency_checks=self.p_consistency_checks)
        sorted_params = sort_dict_by_keys(params)
        return sorted_params

    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            *,
            value: ValueType | Joker,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            retrieve_value: RetrieveValueFlag = IF_ETAG_CHANGED
    ) -> ConditionalOperationResult[ValueType]:
        """Not supported for write-once dictionaries.

        Conditional overwrites (``set_item_if``) contradict write-once
        semantics, which only permit insert-if-absent. Use
        ``setdefault_if`` on the wrapped dict for conditional inserts.

        Raises:
            NotImplementedError: Always raised.
        """
        raise NotImplementedError("Operation not supported on WriteOnceDict.")

    def __setitem__(self, key:NonEmptyPersiDictKey, value: ValueType | Joker) -> None:
        """Set a value for a key, preserving the first assignment.

        Handles joker commands (KEEP_CURRENT, DELETE_CURRENT) before
        attempting the write. KEEP_CURRENT is a no-op; DELETE_CURRENT
        raises KeyError because WriteOnceDict is always append-only.

        For regular values, uses ``setdefault_if`` on the wrapped dict
        for insert-if-absent semantics. Atomicity of the insert depends
        on the wrapped backend (see class docstring). If the key already
        exists, a probabilistic consistency check may be performed to
        ensure the new value matches the originally stored value.

        Args:
            key: Dictionary key.
            value: Value to store, or a joker command.

        Raises:
            KeyError: If value is DELETE_CURRENT (append-only).
            ValueError: If a consistency check is triggered and the new
                value differs from the original value for the key.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return

        p = self.p_consistency_checks
        always_check = (p >= 1.0)

        result = self._wrapped_dict.setdefault_if(
            key,
            default_value=value,
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE if always_check else NEVER_RETRIEVE,
        )

        if not result.value_was_mutated:
            if always_check:
                self._do_consistency_check(key, value, result.new_value)
            elif p > 0 and random.random() < p:
                stored_value = self._wrapped_dict[key]
                self._do_consistency_check(key, value, stored_value)

    def _do_consistency_check(
            self, key: NonEmptyPersiDictKey, new_value: ValueType,
            stored_value: ValueType
    ) -> None:
        """Check that new_value matches the stored value.

        Called when a consistency check has already been selected to run
        (the random gating is handled by the caller). Compares the MD5
        signatures of both values and raises ValueError on mismatch.

        Args:
            key: The key that already exists.
            new_value: The value that was attempted to be written.
            stored_value: The value already retrieved from the backend.

        Raises:
            ValueError: If the values differ.
        """
        self._consistency_checks_attempted += 1
        signature_old = _get_md5_signature(stored_value)
        signature_new = _get_md5_signature(new_value)
        if signature_old != signature_new:
            diff_dict = DeepDiff(stored_value, new_value)
            raise ValueError(
                f"Key {key} is already set "
                + f"to {stored_value} "
                + f"and the new value {new_value} is different, "
                + f"which is not allowed. Details here: {diff_dict} ")
        self._consistency_checks_passed += 1

    def __contains__(self, key:NonEmptyPersiDictKey) -> bool:
        """Check if a key exists in the dictionary.

        Args:
            key: Key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        return key in self._wrapped_dict

    def __getitem__(self, key:NonEmptyPersiDictKey) -> ValueType:
        """Retrieve a value by key.

        Args:
            key: Key to look up.

        Returns:
            Stored value.
        """
        return self._wrapped_dict[key]


    def __len__(self):
        """Return the number of items stored.

        Returns:
            Number of key-value pairs.
        """
        return len(self._wrapped_dict)

    def _generic_iter(self, result_type: set[str]):
        """Delegate iteration to the wrapped dict."""
        return self._wrapped_dict._generic_iter(result_type)

    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Delegate timestamp retrieval to the wrapped dict."""
        return self._wrapped_dict.timestamp(key)

    def __getattr__(self, name):
        """Forward attribute access to the wrapped object.

        Args:
            name: Attribute name.

        Returns:
            Attribute value from the wrapped dict.
        """
        return getattr(self._wrapped_dict, name)


    def __delitem__(self, key):
        """Deletion is not supported for write-once dictionaries.

        Raises:
            TypeError: Always raised to indicate immutable items.
        """
        raise TypeError(
            f"{self.__class__.__name__} has immutable items "
            "and does not support deletion.")


    def get_subdict(self, prefix_key: NonEmptyPersiDictKey) -> 'WriteOnceDict[ValueType]':
        """Return a WriteOnceDict view over a sub-keyspace.

        Args:
            prefix_key: Prefix identifying the sub-dictionary.

        Returns:
            A new WriteOnceDict wrapping the corresponding
                sub-dictionary of the underlying store, sharing the same
                p_consistency_checks probability.
        """
        subdict = self._wrapped_dict.get_subdict(prefix_key)
        result = WriteOnceDict(wrapped_dict=subdict, p_consistency_checks=self.p_consistency_checks)
        return result
