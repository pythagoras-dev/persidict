from __future__ import annotations

import time
from functools import cache

from deepdiff import DeepDiff
from parameterizable import register_parameterizable_class, sort_dict_by_keys

from .jokers import KEEP_CURRENT, KeepCurrentFlag
from .persi_dict import PersiDict
from .file_dir_dict import FileDirDict
import random
import sys
from typing import Any

import joblib.hashing
from .persi_dict import PersiDictKey

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
        str: The base16 MD5 hash of the object.
    """
    if 'numpy' in sys.modules:
        hasher = joblib.hashing.NumpyHasher(hash_name='md5')
    else:
        hasher = joblib.hashing.Hasher(hash_name='md5')
    hash_signature = hasher.hash(x)
    return str(hash_signature)

class WriteOnceDict(PersiDict):
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

    Attributes:
        p_consistency_checks (float): Probability in [0, 1] of performing a
            consistency check for a key that has been previously set.
        consistency_checks_attempted (int): Number of checks that were
            attempted.
        consistency_checks_passed (int): Number of checks that succeeded.
        consistency_checks_failed (int): Derived as attempted - passed.

    """
    _wrapped_dict: PersiDict
    _p_consistency_checks: float | None
    _consistency_checks_attempted: int
    _consistency_checks_passed: int

    def __init__(self,
                 wrapped_dict: PersiDict | None = None,
                 p_consistency_checks: float | None = None):
        """Initialize a WriteOnceDict.

        Args:
            wrapped_dict: The underlying persistent dictionary to wrap. If not
                provided, a FileDirDict with immutable_items=True is created.
            p_consistency_checks: Probability in [0, 1] to perform a
                consistency check when a key already exists. ``None`` means 0.0
                (disabled).

        Raises:
            AssertionError: If ``wrapped_dict`` is not a PersiDict or does not
                enforce immutable items.
        """
        if wrapped_dict is None:
            wrapped_dict = FileDirDict(immutable_items=True)
        assert isinstance(wrapped_dict, PersiDict)
        assert wrapped_dict.immutable_items == True
        self.p_consistency_checks = p_consistency_checks
        PersiDict.__init__(self,
            base_class_for_values=wrapped_dict.base_class_for_values,
            immutable_items=True,
            digest_len=wrapped_dict.digest_len)
        self._wrapped_dict = wrapped_dict
        self._consistency_checks_passed = 0
        self._consistency_checks_attempted = 0


    @property
    def p_consistency_checks(self) -> float:
        """Probability of checking a new value against the first value stored.

        Returns:
            float: Probability in [0, 1].
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
                    f"KEEP_CURRENT can't be used to initialize p_consistency_checks.")
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
            int: Failed checks (attempted - passed).
        """
        return (self._consistency_checks_attempted
                - self._consistency_checks_passed)


    @property
    def consistency_checks_attempted(self) -> int:
        """Number of attempted consistency checks.

        Returns:
            int: Attempted checks counter.
        """
        return self._consistency_checks_attempted


    @property
    def consistency_checks_passed(self) -> int:
        """Number of successful consistency checks.

        Returns:
            int: Passed checks counter.
        """
        return self._consistency_checks_passed


    def get_params(self):
        """Return parameterization of this instance.

        Returns:
            dict: A dictionary with keys 'wrapped_dict' and
                'p_consistency_checks', sorted by keys for deterministic
                comparison/serialization.
        """
        params = dict(
            wrapped_dict=self._wrapped_dict,
            p_consistency_checks=self.p_consistency_checks)
        sorted_params = sort_dict_by_keys(params)
        return sorted_params

    def __setitem__(self, key:PersiDictKey, value):
        """Set a value for a key, preserving the first assignment.

        If the key is new, the value is stored. If the key already exists,
        a probabilistic consistency check may be performed to ensure the new
        value matches the originally stored value. If a check is performed and
        the values differ, a ValueError is raised.

        Args:
            key: key (string or sequence of strings or SafeStrTuple)
            value: Value to store.

        Raises:
            KeyError: If the wrapped dict failed to set a new key unexpectedly.
            ValueError: If a consistency check is triggered and the new value
                differs from the original value for the key.
        """
        check_needed = False

        n_retries = 8
        for i in range(n_retries):
            try:  # extra protections to better handle concurrent writes
                if key in self._wrapped_dict:
                    check_needed = True
                else:
                    self._wrapped_dict[key] = value
                break
            except Exception as e:
                if i < n_retries - 1:
                    time.sleep(random.uniform(0.01, 0.1) * (2 ** i))
                else:
                    raise e

        if not key in self._wrapped_dict:
            raise KeyError(
                f"Key {key} was not set in the wrapped dict "
                + f"{self._wrapped_dict}. This should not happen.")

        if check_needed and self.p_consistency_checks > 0:
            if random.random() < self.p_consistency_checks:
                self._consistency_checks_attempted += 1
                signature_old = _get_md5_signature(self._wrapped_dict[key])
                signature_new = _get_md5_signature(value)
                if signature_old != signature_new:
                    diff_dict = DeepDiff(self._wrapped_dict[key], value)
                    raise ValueError(
                        f"Key {key} is already set "
                        + f"to {self._wrapped_dict[key]} "
                        + f"and the new value {value} is different, "
                        + f"which is not allowed. Details here: {diff_dict} ")
                self._consistency_checks_passed += 1

    def __contains__(self, item:PersiDictKey):
        """Check if a key exists in the dictionary.

        Args:
            item: Key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return item in self._wrapped_dict

    def __getitem__(self, key:PersiDictKey):
        """Retrieve a value by key.

        Args:
            key: Key to look up.

        Returns:
            Any: Stored value.
        """
        return self._wrapped_dict[key]

    def __len__(self):
        """Return the number of items stored.

        Returns:
            int: Number of key-value pairs.
        """
        return len(self._wrapped_dict)

    def _generic_iter(self, iter_type: set[str]):
        """Delegate iteration to the wrapped dict.

        Args:
            iter_type: tType of iterator: 'items' and/or 'keys' and/or 'timestamps'.

        Returns:
            Any: Iterator from the wrapped dictionary.
        """
        return self._wrapped_dict._generic_iter(iter_type)

    def timestamp(self, key: PersiDictKey) -> float:
        """Return the timestamp for a given key.

        Args:
            key: Key for which to retrieve the timestamp.

        Returns:
            float: POSIX timestamp (seconds since epoch) of the item's last
                modification as tracked by the wrapped dict.
        """
        return self._wrapped_dict.timestamp(key)

    def __getattr__(self, name):
        """Forward attribute access to the wrapped object.

        Args:
            name: Attribute name.

        Returns:
            Any: Attribute value from the wrapped dict.
        """
        return getattr(self._wrapped_dict, name)

    @property
    def base_dir(self):
        """Base directory of the wrapped dict (if applicable)."""
        return self._wrapped_dict.base_dir

    @property
    def base_url(self):
        """Base URL of the wrapped dict (if applicable)."""
        return self._wrapped_dict.base_url

    def get_subdict(self, prefix_key: PersiDictKey) -> WriteOnceDict:
        """Return a WriteOnceDict view over a sub-keyspace.

        Args:
            prefix_key: Prefix identifying the sub-dictionary.

        Returns:
            WriteOnceDict: A new WriteOnceDict wrapping the corresponding
                sub-dictionary of the underlying store, sharing the same
                p_consistency_checks probability.
        """
        subdict = self._wrapped_dict.get_subdict(prefix_key)
        result = WriteOnceDict(subdict, self.p_consistency_checks)
        return result

register_parameterizable_class(WriteOnceDict)