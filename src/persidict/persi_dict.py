"""Persistent, dict-like API for durable key-value stores.

PersiDict defines a unified interface for persistent dictionaries. The API is
similar to Python's built-in dict with some differences (e.g., insertion order
is not guaranteed) and several additional convenience methods.

Keys are non-empty sequences of URL/filename-safe strings
represented by SafeStrTuple. Plain strings or sequences of strings are accepted
and automatically coerced to SafeStrTuple. Values can be
arbitrary Python objects unless an implementation restricts them
via `base_class_for_values`.

Persistence means items are stored durably (e.g., in local files or cloud
objects) and remain accessible across process lifetimes.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import MutableMapping
import heapq
from itertools import zip_longest
import random
import time
from typing import Any, Sequence, Optional, TypeVar, Iterator, Mapping

from mixinforge import ParameterizableMixin, sort_dict_by_keys

from . import NonEmptySafeStrTuple
from .jokers_and_status_flags import (KEEP_CURRENT, DELETE_CURRENT, Joker,
                                      CONTINUE_NORMAL_EXECUTION, StatusFlag, EXECUTION_IS_COMPLETE,
                                      ETagValue, ETagConditionFlag,
                                      ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED,
                                      ITEM_NOT_AVAILABLE, VALUE_NOT_RETRIEVED,
                                      ETagIfExists, TransformingFunction,
                                      OperationResult, ConditionalOperationResult)
from .safe_chars import contains_unsafe_chars
from .safe_str_tuple import SafeStrTuple

ValueType = TypeVar('ValueType')
"""Generic type variable for dictionary values.

This TypeVar is used to make PersiDict and its subclasses generic over
the value type, enabling static type checking with tools like mypy.

Example:
    d: FileDirDict[int] = FileDirDict(base_dir="./data")
    val: int = d["key"]  # Type checker knows this is int
"""

PersiDictKey = SafeStrTuple | Sequence[str] | str
NonEmptyPersiDictKey = NonEmptySafeStrTuple | Sequence[str] | str
"""A value which can be used as a key for PersiDict.

PersiDict instances accept keys in the form of (NonEmpty)SafeStrTuple,
or a string, or a (non-empty) sequence of strings.
The characters within strings must be URL/filename-safe.
If a string (or a sequence of strings) is passed to a PersiDict as a key,
it will be automatically converted into SafeStrTuple.
"""

class TransformConflictError(RuntimeError):
    """Raised when transform_item exhausts retries due to concurrent updates."""

    def __init__(self, key: NonEmptySafeStrTuple, attempts: int) -> None:
        super().__init__(
            f"transform_item failed after {attempts} attempt(s) for key {key!r}")
        self.key = key
        self.attempts = attempts


class PersiDict(MutableMapping[NonEmptySafeStrTuple, ValueType], ParameterizableMixin):
    """Abstract dict-like interface for durable key-value stores.

    Keys are URL/filename-safe sequences of strings (SafeStrTuple). Concrete
    subclasses implement storage backends (e.g., filesystem, S3). The API is
    similar to Python's dict but does not guarantee insertion order and adds
    persistence-specific helpers (e.g., timestamp()).

    Attributes (can't be changed after initialization):
        append_only (bool):
            If True, items are immutable and non-removable: existing values 
            cannot be modified or deleted.
        base_class_for_values (Optional[type]):
            Optional base class that all values must inherit from. If None, any
            type is accepted.
        serialization_format (str):
            File extension/format for stored values (e.g., "pkl", "json").
    """

    append_only:bool
    base_class_for_values:Optional[type]
    serialization_format:str

    def __init__(self,
                 append_only: bool = False,
                 base_class_for_values: type|None = None,
                 serialization_format: str = "pkl",
                 *args, **kwargs):
        """Initialize base parameters shared by all persistent dictionaries.

        Args:
            append_only: If True, items cannot be modified or deleted.
                Defaults to False.
            base_class_for_values: Optional base class that values
                must inherit from. If None, values are not type-restricted.
                Defaults to None.
            serialization_format: File extension/format for stored values.
                Defaults to "pkl".
            *args: Additional positional arguments (ignored in base class, reserved
                for subclasses).
            **kwargs: Additional keyword arguments (ignored in base class, reserved
                for subclasses).

        Raises:
            ValueError: If serialization_format is an empty string,
            or contains unsafe characters, or not 'json' or 'pkl'
            for non-string values.

            TypeError: If base_class_for_values is not a type or None.
        """


        self._append_only = bool(append_only)
        
        if len(serialization_format) == 0:
            raise ValueError("serialization_format must be a non-empty string")
        if contains_unsafe_chars(serialization_format):
            raise ValueError("serialization_format must contain only URL/filename-safe characters")
        self.serialization_format = str(serialization_format)

        if not isinstance(base_class_for_values, (type, type(None))):
            raise TypeError("base_class_for_values must be a type or None")
        if (base_class_for_values is None or
                not issubclass(base_class_for_values, str)):
            if serialization_format not in {"json", "pkl"}:
                raise ValueError("For non-string values serialization_format must be either 'pkl' or 'json'.")
        self.base_class_for_values = base_class_for_values

        ParameterizableMixin.__init__(self)


    def get_params(self):
        """Return configuration parameters of this dictionary.

        Returns:
            dict: A sorted dictionary of parameters used to reconstruct the instance.
                This supports the Parameterizable API and is absent in the
                built-in dict.
        """
        params = dict(
            append_only=self.append_only,
            base_class_for_values=self.base_class_for_values,
            serialization_format=self.serialization_format
        )
        sorted_params = sort_dict_by_keys(params)
        return sorted_params

    def __copy__(self) -> 'PersiDict[ValueType]':
        """Return a shallow copy of the dictionary.

        This creates a new PersiDict instance with the same parameters, pointing
        to the same underlying storage. This is analogous to `dict.copy()`.

        Returns:
            PersiDict[ValueType]: A new PersiDict instance that is a shallow copy of this one.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot be copied directly")
        params = self.get_params()
        return self.__class__(**params)


    @property
    def append_only(self) -> bool:
        """Whether the store is append-only.

        Returns:
            bool: True if the store is append-only (contains immutable items
            that cannot be modified or deleted), False otherwise.
        """
        return self._append_only


    def __repr__(self) -> str:
        """Return a reproducible string representation.

        Returns:
            str: Representation including class name and constructor parameters.
        """
        params = self.get_params()
        params_str = ', '.join(f'{k}={v!r}' for k, v in params.items())
        return f'{self.__class__.__name__}({params_str})'


    def __str__(self) -> str:
        """Return a user-friendly string with all items.

        Returns:
            str: Stringified dict of items.
        """
        return str(dict(self.items()))


    def __bool__(self) -> bool:
        """Return True if the dictionary is non-empty, False otherwise.

        This provides an efficient truth-value check that avoids calling
        __len__(), which can be expensive for large persistent stores
        (e.g., full directory traversal for FileDirDict, S3 pagination
        for BasicS3Dict). Instead, it attempts to retrieve just the first
        key using the streaming iterator.

        Returns:
            bool: True if at least one key exists; False if empty.
        """
        try:
            next(iter(self))
            return True
        except StopIteration:
            return False


    @abstractmethod
    def __contains__(self, key:NonEmptyPersiDictKey) -> bool:
        """Check whether a key exists in the store.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if key exists, False otherwise.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                    " and cannot check items directly")


    def _check_condition(
            self,
            condition: ETagConditionFlag,
            expected_etag: ETagIfExists,
            actual_etag: ETagIfExists
    ) -> bool:
        """Evaluate an ETag condition.

        Args:
            condition: The condition to check (ANY_ETAG, ETAG_IS_THE_SAME,
                or ETAG_HAS_CHANGED).
            expected_etag: The caller's expected ETag value, or
                ITEM_NOT_AVAILABLE if the caller believes the key is absent.
            actual_etag: The actual ETag value, or ITEM_NOT_AVAILABLE if
                the key is absent.

        Returns:
            bool: True if the condition is satisfied.

        Raises:
            ValueError: If condition is not a recognized ETagConditionFlag.
        """
        if condition is ANY_ETAG:
            return True
        if condition is ETAG_IS_THE_SAME:
            return expected_etag == actual_etag
        if condition is ETAG_HAS_CHANGED:
            return expected_etag != actual_etag
        raise ValueError(
            f"condition must be ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED, got {condition!r}")

    def _actual_etag(self, key: NonEmptySafeStrTuple) -> ETagIfExists:
        """Return the actual ETag for a key, or ITEM_NOT_AVAILABLE if absent.

        Args:
            key: Normalized dictionary key.

        Returns:
            ETagIfExists: The ETag value, or ITEM_NOT_AVAILABLE if the key
                does not exist.
        """
        try:
            return self.etag(key)
        except (KeyError, FileNotFoundError):
            return ITEM_NOT_AVAILABLE

    def _get_value_and_etag(self, key: NonEmptySafeStrTuple) -> tuple[ValueType, ETagValue]:
        """Return the value and ETag for a key.

        Subclasses can override to fetch both in a single backend pass.

        Args:
            key: Normalized dictionary key.

        Returns:
            tuple[ValueType, ETagValue]: The value and its current ETag.

        Raises:
            KeyError: If the key does not exist.
        """
        value = self[key]
        actual_etag = self.etag(key)
        return value, actual_etag

    # --- ConditionalOperationResult factory methods ---

    @staticmethod
    def _result_item_not_available(
            condition: ETagConditionFlag,
            satisfied: bool
    ) -> ConditionalOperationResult:
        """Build result when the key is absent."""
        return ConditionalOperationResult(
            condition_was_satisfied=satisfied,
            requested_condition=condition,
            actual_etag=ITEM_NOT_AVAILABLE,
            resulting_etag=ITEM_NOT_AVAILABLE,
            new_value=ITEM_NOT_AVAILABLE)

    @staticmethod
    def _result_unchanged(
            condition: ETagConditionFlag,
            satisfied: bool,
            actual_etag: ETagIfExists,
            new_value: Any
    ) -> ConditionalOperationResult:
        """Build result when no mutation occurred (resulting_etag == actual_etag)."""
        return ConditionalOperationResult(
            condition_was_satisfied=satisfied,
            requested_condition=condition,
            actual_etag=actual_etag,
            resulting_etag=actual_etag,
            new_value=new_value)

    @staticmethod
    def _result_write_success(
            condition: ETagConditionFlag,
            actual_etag: ETagIfExists,
            resulting_etag: ETagValue,
            new_value: Any
    ) -> ConditionalOperationResult:
        """Build result for a successful write."""
        return ConditionalOperationResult(
            condition_was_satisfied=True,
            requested_condition=condition,
            actual_etag=actual_etag,
            resulting_etag=resulting_etag,
            new_value=new_value)

    @staticmethod
    def _result_delete_success(
            condition: ETagConditionFlag,
            actual_etag: ETagIfExists
    ) -> ConditionalOperationResult:
        """Build result for a successful delete."""
        return ConditionalOperationResult(
            condition_was_satisfied=True,
            requested_condition=condition,
            actual_etag=actual_etag,
            resulting_etag=ITEM_NOT_AVAILABLE,
            new_value=ITEM_NOT_AVAILABLE)

    def get_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Retrieve the value for a key only if an ETag condition is satisfied.

        If the key is absent, actual_etag is ITEM_NOT_AVAILABLE and the
        condition is evaluated normally. No KeyError is raised.

        Warning:
            This base class implementation is not atomic. Subclasses that
            offer concurrency safety should override this method.

        Args:
            key: Dictionary key.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE
                if the caller believes the key is absent.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            always_retrieve_value: If True (default), new_value always
                reflects the actual state. If False, VALUE_NOT_RETRIEVED is
                returned when the key exists and the value is already known
                to the caller (expected_etag == actual_etag).

        Returns:
            ConditionalOperationResult with the outcome of the operation.
        """
        key = NonEmptySafeStrTuple(key)
        if always_retrieve_value:
            try:
                value, actual_etag = self._get_value_and_etag(key)
            except (KeyError, FileNotFoundError):
                satisfied = self._check_condition(
                    condition, expected_etag, ITEM_NOT_AVAILABLE)
                return self._result_item_not_available(condition, satisfied)
            satisfied = self._check_condition(condition, expected_etag, actual_etag)
            return self._result_unchanged(condition, satisfied, actual_etag, value)

        actual_etag = self._actual_etag(key)
        if actual_etag is ITEM_NOT_AVAILABLE:
            satisfied = self._check_condition(condition, expected_etag, actual_etag)
            return self._result_item_not_available(condition, satisfied)

        if expected_etag == actual_etag:
            satisfied = self._check_condition(condition, expected_etag, actual_etag)
            return self._result_unchanged(
                condition, satisfied, actual_etag, VALUE_NOT_RETRIEVED)

        try:
            value, actual_etag = self._get_value_and_etag(key)
        except (KeyError, FileNotFoundError):
            satisfied = self._check_condition(
                condition, expected_etag, ITEM_NOT_AVAILABLE)
            return self._result_item_not_available(condition, satisfied)

        satisfied = self._check_condition(condition, expected_etag, actual_etag)
        return self._result_unchanged(condition, satisfied, actual_etag, value)

    def set_item_if(
            self,
            key: NonEmptyPersiDictKey,
            value: ValueType,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Store a value only if an ETag condition is satisfied.

        If the key is absent, actual_etag is ITEM_NOT_AVAILABLE and the
        condition is evaluated normally. No KeyError is raised.

        Warning:
            This base class implementation is not atomic. Subclasses that
            require concurrency safety should override this method.

        Args:
            key: Dictionary key.
            value: Value to store.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE
                if the caller believes the key is absent.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            always_retrieve_value: If True (default), the existing value is
                returned when the condition fails and the key exists. If False,
                VALUE_NOT_RETRIEVED is returned instead.

        Returns:
            ConditionalOperationResult with the outcome of the operation.
        """
        key = NonEmptySafeStrTuple(key)
        actual_etag = self._actual_etag(key)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if not satisfied:
            if actual_etag is ITEM_NOT_AVAILABLE:
                return self._result_item_not_available(condition, False)
            existing_value = self[key] if always_retrieve_value else VALUE_NOT_RETRIEVED
            return self._result_unchanged(condition, False, actual_etag, existing_value)

        if value is KEEP_CURRENT:
            return self._result_unchanged(
                condition, True, actual_etag, VALUE_NOT_RETRIEVED)

        if value is DELETE_CURRENT:
            self.discard(key)
            return self._result_delete_success(condition, actual_etag)

        self[key] = value
        resulting_etag = self._actual_etag(key)
        return self._result_write_success(
            condition, actual_etag, resulting_etag, value)

    def setdefault_if(
            self,
            key: NonEmptyPersiDictKey,
            default_value: ValueType,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag,
            *,
            always_retrieve_value: bool = True
    ) -> ConditionalOperationResult:
        """Insert default_value if key is absent; conditioned on ETag check.

        If the key is absent and the condition is satisfied, default_value
        is inserted. If the key is present, no mutation occurs regardless
        of whether the condition is satisfied.

        Warning:
            This base class implementation is not atomic. Subclasses that
            require concurrency safety should override this method.

        Args:
            key: Dictionary key.
            default_value: Value to insert if the key is absent and the
                condition is satisfied.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE
                if the caller believes the key is absent.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.
            always_retrieve_value: If True (default), the existing value is
                returned when the key exists. If False, VALUE_NOT_RETRIEVED
                is returned instead.

        Returns:
            ConditionalOperationResult with the outcome of the operation.
        """
        if isinstance(default_value, Joker):
            raise TypeError("default_value must be a regular value, not a Joker command")
        key = NonEmptySafeStrTuple(key)
        actual_etag = self._actual_etag(key)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if actual_etag is ITEM_NOT_AVAILABLE:
            if satisfied:
                self[key] = default_value
                resulting_etag = self._actual_etag(key)
                return self._result_write_success(
                    condition, ITEM_NOT_AVAILABLE, resulting_etag, default_value)
            else:
                return self._result_item_not_available(condition, False)

        existing_value = self[key] if always_retrieve_value else VALUE_NOT_RETRIEVED
        return self._result_unchanged(
            condition, satisfied, actual_etag, existing_value)

    def discard_item_if(
            self,
            key: NonEmptyPersiDictKey,
            expected_etag: ETagIfExists,
            condition: ETagConditionFlag
    ) -> ConditionalOperationResult:
        """Discard a key only if an ETag condition is satisfied.

        No always_retrieve_value parameter — new_value is ITEM_NOT_AVAILABLE
        on delete success or missing key; on condition failure it is
        VALUE_NOT_RETRIEVED.

        Warning:
            This base class implementation is not atomic. Subclasses that
            require concurrency safety should override this method.

        Args:
            key: Dictionary key.
            expected_etag: The caller's expected ETag, or ITEM_NOT_AVAILABLE
                if the caller believes the key is absent.
            condition: ANY_ETAG, ETAG_IS_THE_SAME, or ETAG_HAS_CHANGED.

        Returns:
            ConditionalOperationResult with the outcome of the operation.
        """
        key = NonEmptySafeStrTuple(key)
        actual_etag = self._actual_etag(key)
        satisfied = self._check_condition(condition, expected_etag, actual_etag)

        if actual_etag is ITEM_NOT_AVAILABLE:
            return self._result_item_not_available(condition, satisfied)

        if satisfied:
            self.discard(key)
            return self._result_delete_success(condition, actual_etag)

        return self._result_unchanged(
            condition, False, actual_etag, VALUE_NOT_RETRIEVED)

    def transform_item(
            self,
            key: NonEmptyPersiDictKey,
            transformer: TransformingFunction,
            *,
            n_retries: int | None = 6
    ) -> OperationResult:
        """Apply a transformation function to a key's value.

        Reads the current value (or ITEM_NOT_AVAILABLE if absent), calls
        transformer(current_value), and writes the result back using
        conditional operations.

        If the transformer returns DELETE_CURRENT, the key is deleted
        (or no-op if already absent). If the transformer returns
        KEEP_CURRENT, the value is left unchanged.

        Warning:
            This base class implementation is not atomic unless the backend's
            conditional operations are atomic. The transformer may be called
            multiple times if conflicts occur.

        Args:
            key: Dictionary key.
            transformer: A callable that receives the current value (or
                ITEM_NOT_AVAILABLE) and returns a new value,
                DELETE_CURRENT, or KEEP_CURRENT.
            n_retries: Number of retries after ETag conflicts. None retries
                indefinitely.

        Raises:
            TransformConflictError: If conflicts persist after n_retries.

        Returns:
            OperationResult with resulting_etag and new_value.
        """
        key = NonEmptySafeStrTuple(key)
        if n_retries is not None:
            try:
                n_retries = int(n_retries)
            except (TypeError, ValueError) as exc:
                raise TypeError("n_retries must be a non-negative int or None") from exc
            if n_retries < 0:
                raise ValueError("n_retries must be a non-negative int or None")

        retries = 0
        while True:
            read_res = self.get_item_if(
                key,
                ITEM_NOT_AVAILABLE,
                ANY_ETAG,
                always_retrieve_value=True)
            current_value = read_res.new_value
            actual_etag = read_res.actual_etag

            new_value = transformer(current_value)

            if new_value is KEEP_CURRENT:
                return OperationResult(
                    resulting_etag=actual_etag,
                    new_value=current_value)

            if new_value is DELETE_CURRENT:
                delete_res = self.discard_item_if(
                    key, actual_etag, ETAG_IS_THE_SAME)
                if delete_res.condition_was_satisfied:
                    return OperationResult(
                        resulting_etag=ITEM_NOT_AVAILABLE,
                        new_value=ITEM_NOT_AVAILABLE)
            else:
                write_res = self.set_item_if(
                    key,
                    new_value,
                    actual_etag,
                    ETAG_IS_THE_SAME,
                    always_retrieve_value=False)
                if write_res.condition_was_satisfied:
                    return OperationResult(
                        resulting_etag=write_res.resulting_etag,
                        new_value=new_value)

            if n_retries is not None and retries >= n_retries:
                raise TransformConflictError(key, retries + 1)

            time.sleep(random.uniform(0.01, 0.2) * (1.75 ** retries))
            retries += 1


    @abstractmethod
    def __getitem__(self, key:NonEmptyPersiDictKey) -> ValueType:
        """Retrieve the value for a key.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            ValueType: The stored value.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot retrieve items directly")



    def _validate_value(self, value: ValueType) -> None:
        """Validate that a value is acceptable for storage.

        Joker commands (KEEP_CURRENT, DELETE_CURRENT) are silently accepted.

        Args:
            value: Value to store, or a joker command.

        Raises:
            TypeError: If the value is a PersiDict instance or does not match
                the required base_class_for_values when specified.
        """
        if isinstance(value, PersiDict):
            raise TypeError("Cannot store a PersiDict instance directly")
        if value is not KEEP_CURRENT and value is not DELETE_CURRENT:
            if self.base_class_for_values is not None:
                if not isinstance(value, self.base_class_for_values):
                    raise TypeError(f"Value must be an instance of"
                                    f" {self.base_class_for_values.__name__}")

    def _validate_setitem_args(self, key: NonEmptyPersiDictKey, value: ValueType | Joker
                               ) -> NonEmptySafeStrTuple:
        """Validate setitem arguments without applying joker side effects.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True (except for KEEP_CURRENT).
            TypeError: If the value is a PersiDict instance or does not match
                the required base_class_for_values when specified.

        Returns:
            NonEmptySafeStrTuple: Normalized key.
        """

        if self.append_only and value is not KEEP_CURRENT:
            if value is DELETE_CURRENT or key in self:
                raise KeyError("Can't modify an immutable key-value pair")

        self._validate_value(value)
        key = NonEmptySafeStrTuple(key)
        return key

    def _process_setitem_args(self, key: NonEmptyPersiDictKey, value: ValueType | Joker
                              ) -> StatusFlag:
        """Perform the first steps for setting an item.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
            value: Value to store, or a joker command (KEEP_CURRENT or
                DELETE_CURRENT).

        Raises:
            KeyError: If attempting to modify an existing item when
                append_only is True.
            TypeError: If the value is a PersiDict instance or does not match
                the required base_class_for_values when specified.

        Returns:
            StatusFlag: CONTINUE_NORMAL_EXECUTION if the caller should
                proceed with storing the value; EXECUTION_IS_COMPLETE if a
                joker command was processed and no further action is needed.
        """

        if value is KEEP_CURRENT:
            return EXECUTION_IS_COMPLETE

        key = self._validate_setitem_args(key, value)
        if value is DELETE_CURRENT:
            self.discard(key)
            return EXECUTION_IS_COMPLETE

        return CONTINUE_NORMAL_EXECUTION


    @abstractmethod
    def __setitem__(self, key:NonEmptyPersiDictKey, value: ValueType | Joker) -> None:
        """Set the value for a key.

        Special values KEEP_CURRENT and DELETE_CURRENT are interpreted as
        commands to keep or delete the current value respectively.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.
            value: Value to store, or a Joker command.

        Raises:
            KeyError: If attempting to modify an existing key when
                append_only is True.
            NotImplementedError: Subclasses must implement actual writing.
        """
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return

        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot store items directly")


    def _process_delitem_args(self, key: NonEmptyPersiDictKey) -> None:
        """Perform the first steps for deleting an item.

        Args:
            key: Dictionary key (string or sequence of strings
                or NonEmptySafeStrTuple).
        Raises:
            KeyError: If attempting to delete an item when
                append_only is True or if the key does not exist.
        """
        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        key = NonEmptySafeStrTuple(key)

        if key not in self:
            raise KeyError(f"Key {key} not found")


    @abstractmethod
    def __delitem__(self, key:NonEmptyPersiDictKey):
        """Delete a key and its value.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Raises:
            KeyError: If append_only is True or if the key does not exist.
            NotImplementedError: Subclasses must implement deletion.
        """
        self._process_delitem_args(key)
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot delete items directly")


    @abstractmethod
    def __len__(self) -> int:
        """Return the number of stored items.

        Returns:
            int: Number of key-value pairs.
        Raises:
            NotImplementedError: Subclasses must implement counting.
        """
        raise NotImplementedError("PersiDict is an abstract base class"
                                    " and cannot count items directly")


    def _process_generic_iter_args(self, result_type: set[str]) -> None:
        """Validate arguments for iterator helpers.

        Args:
            result_type: A set indicating desired fields among {'keys',
                'values', 'timestamps'}.
        Raises:
            TypeError: If result_type is not a set.
            ValueError: If result_type contains invalid entries or an invalid number of items.
        """
        if not isinstance(result_type, set):
            raise TypeError("result_type must be a set of strings")
        if not (1 <= len(result_type) <= 3):
            raise ValueError("result_type must contain between 1 and 3 elements")
        allowed = {"keys", "values", "timestamps"}
        if (result_type | allowed) != allowed:
            raise ValueError("result_type can only contain 'keys', 'values', 'timestamps'")
        if not (1 <= len(result_type & allowed) <= 3):
            raise ValueError("result_type must include at least one of 'keys', 'values', 'timestamps'")


    @abstractmethod
    def _generic_iter(self, result_type: set[str]) -> Any:
        """Underlying implementation for iterator helpers.

        Args:
            result_type: A set indicating desired fields among {'keys',
                'values', 'timestamps'}.

        Returns:
            Any: An iterator yielding keys, values, and/or timestamps based on
                result_type.

        Raises:
            TypeError: If result_type is not a set.
            ValueError: If result_type contains invalid entries or an invalid number of items.
            NotImplementedError: Subclasses must implement the concrete iterator.
        """
        self._process_generic_iter_args(result_type)
        raise NotImplementedError("PersiDict is an abstract base class"
                                  " and cannot iterate items directly")


    def __iter__(self) -> Iterator[NonEmptySafeStrTuple]:
        """Iterate over keys.

        Returns:
            Iterator[NonEmptySafeStrTuple]: Iterator of keys.
        """
        return self._generic_iter({"keys"})


    def keys(self) -> Iterator[NonEmptySafeStrTuple]:
        """Return an iterator over keys.

        Returns:
            Iterator[NonEmptySafeStrTuple]: Keys iterator.
        """
        return self._generic_iter({"keys"})


    def keys_and_timestamps(self) -> Iterator[tuple[NonEmptySafeStrTuple, float]]:
        """Return an iterator over (key, timestamp) pairs.

        Returns:
            Iterator[tuple[NonEmptySafeStrTuple, float]]: Keys and POSIX timestamps.
        """
        return self._generic_iter({"keys", "timestamps"})


    def values(self) -> Iterator[ValueType]:
        """Return an iterator over values.

        Returns:
            Iterator[ValueType]: Values iterator.
        """
        return self._generic_iter({"values"})


    def values_and_timestamps(self) -> Iterator[tuple[ValueType, float]]:
        """Return an iterator over (value, timestamp) pairs.

        Returns:
            Iterator[tuple[ValueType, float]]: Values and POSIX timestamps.
        """
        return self._generic_iter({"values", "timestamps"})


    def items(self) -> Iterator[tuple[NonEmptySafeStrTuple, ValueType]]:
        """Return an iterator over (key, value) pairs.

        Returns:
            Iterator[tuple[NonEmptySafeStrTuple, ValueType]]: Items iterator.
        """
        return self._generic_iter({"keys", "values"})


    def items_and_timestamps(self) -> Iterator[tuple[NonEmptySafeStrTuple, ValueType, float]]:
        """Return an iterator over (key, value, timestamp) triples.

        Returns:
            Iterator[tuple[NonEmptySafeStrTuple, ValueType, float]]: Items and timestamps.
        """
        return self._generic_iter({"keys", "values", "timestamps"})


    def setdefault(self, key: NonEmptyPersiDictKey, default: ValueType | None = None) -> ValueType:
        """Insert key with default value if absent; return the current value.

        Behaves like the built-in dict.setdefault() method: if the key exists,
        return its current value; otherwise, set the key to the default value
        and return that default.

        Args:
            key: Key (string, sequence of strings, or SafeStrTuple).
            default: Value to insert if the key is not present. Defaults to None.

        Returns:
            Existing value if key is present; otherwise the provided default value.

        Raises:
            TypeError: If default is a Joker command (KEEP_CURRENT/DELETE_CURRENT).
        """
        key = NonEmptySafeStrTuple(key)
        if isinstance(default, Joker):
            raise TypeError("default must be a regular value, not a Joker command")
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def __eq__(self, other: Any) -> bool:
        """Compare dictionaries for equality.

        If other is a PersiDict instance, compares parameters for equality.
        Otherwise, attempts to compare as a mapping by comparing all keys and values.

        Args:
            other: Another dictionary-like object to compare against.

        Returns:
            True if the dictionaries kave the same key/value pairs, False otherwise.
        """
        if self is other:
            return True

        if not isinstance(other, Mapping):
            return NotImplemented

        try:
            if type(self) is type(other) :
                if self.get_params() == other.get_params():
                    return True

            for self_key, other_key_value in zip_longest(
                    self.keys(),other.items(), fillvalue=None):
                if self_key is None or other_key_value is None:
                    return False
                (other_key, other_value) = other_key_value
                if self[other_key] != other_value:
                    return False

        except (KeyError,TypeError, AttributeError, ValueError):
            return False

        return True

    def __ne__(self, other: Any) -> bool:
        if self is other:
            return False

        eq_result = self.__eq__(other)
        if eq_result is NotImplemented:
            return NotImplemented
        return not eq_result

    def __ior__(self, other: Mapping) -> 'PersiDict':
        """Update this dict with items from other (self |= other)."""
        if not isinstance(other, Mapping):
            raise TypeError(f"Cannot update PersiDict with non-Mapping type: {type(other)}")
        self.update(other)
        return self


    def __getstate__(self):
        """Prevent pickling of PersiDict instances.

        Raises:
            TypeError: Always raised; PersiDict instances are not pickleable.
        """
        raise TypeError(
            f"{self.__class__.__name__} instances cannot be pickled. "
            "To persist configuration, use get_params().")


    def __setstate__(self, state):
        """Prevent unpickling of PersiDict instances.

        Raises:
            TypeError: Always raised; PersiDict instances are not pickleable.
        """
        raise TypeError(
            f"{self.__class__.__name__} instances cannot be unpickled. "
            "Recreate from parameters instead.")


    def clear(self) -> None:
        """Remove all items from the dictionary.

        Raises:
            KeyError: If the dictionary is append-only.
        """
        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        for k in self.keys():
            try:
                del self[k]
            except KeyError:
                pass


    def discard(self, key: NonEmptyPersiDictKey) -> bool:
        """Delete an item without raising an exception if it doesn't exist.

        This method is absent in the original dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            bool: True if the item existed and was deleted; False otherwise.

        Raises:
            KeyError: If the dictionary is append-only.
        """

        if self.append_only:
            raise KeyError("Can't delete an immutable key-value pair")

        key = NonEmptySafeStrTuple(key)

        try:
            del self[key]
            return True
        except KeyError:
            return False


    def delete_if_exists(self, key: NonEmptyPersiDictKey) -> bool:
        """Backward-compatible wrapper for discard().

        This method is kept for backward compatibility; new code should use
        discard(). Behavior is identical to discard().
        """
        return self.discard(key)

    def get_subdict(self, prefix_key:PersiDictKey) -> 'PersiDict[ValueType]':
        """Get a sub-dictionary containing items with the given prefix key.

        Items whose keys start with the provided prefix are visible through the
        returned sub-dictionary. If the prefix does not exist, an empty
        sub-dictionary is returned. If the prefix is empty, the entire
        dictionary is returned.

        This method is absent in the original Python dict API.

        Args:
            prefix_key: Key prefix (string, sequence of strings, or SafeStrTuple)
                identifying the sub-dict to expose.

        Returns:
            PersiDict[ValueType]: A dictionary-like view restricted to keys under the
                provided prefix.

        Raises:
            NotImplementedError: Must be implemented by subclasses that support
                hierarchical key spaces.
        """

        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                " and cannot create sub-dictionaries directly")


    def subdicts(self) -> dict[str, 'PersiDict[ValueType]']:
        """Return a mapping of first-level keys to sub-dictionaries.

        This method is absent in the original dict API.

        Returns:
            dict[str, PersiDict[ValueType]]: A mapping from a top-level key segment to a
                sub-dictionary restricted to the corresponding keyspace.
        """
        all_keys = {k[0] for k in self.keys()}
        result_subdicts = {k: self.get_subdict(k) for k in all_keys}
        return result_subdicts


    def random_key(self) -> NonEmptySafeStrTuple | None:
        """Return a random key from the dictionary.

        This method is absent in the original Python dict API.

        Implementation uses reservoir sampling to select a uniformly random key
        in streaming time, without loading all keys into memory or using len().

        Returns:
            SafeStrTuple | None: A random key if the dictionary is not empty;
                None if the dictionary is empty.
        """
        iterator = iter(self.keys())
        try:
            # Get the first key
            result = next(iterator)
        except StopIteration:
            # Dictionary is empty
            return None

        # Reservoir sampling algorithm
        i = 2
        for key in iterator:
            # Select the current key with probability 1/i
            if random.random() < 1/i:
                result = key
            i += 1

        return result


    @abstractmethod
    def timestamp(self, key:NonEmptyPersiDictKey) -> float:
        """Return the last modification time of a key.

        This method is absent in the original dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            float: POSIX timestamp (seconds since Unix epoch) of the last
                modification of the item.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        if type(self) is PersiDict:
            raise NotImplementedError("PersiDict is an abstract base class"
                                      " and cannot provide timestamps directly")

    def etag(self, key: NonEmptyPersiDictKey) -> ETagValue:
        """Return the ETag of a key.

        By default, this returns a stringified timestamp of the last
        modification time. Subclasses may override to provide true
        backend-specific ETags (e.g., S3).

        This method is absent in the original Python dict API.

        Args:
            key: Key (string or sequence of strings) or SafeStrTuple.

        Returns:
            ETagValue: The ETag for the key.

        Raises:
            KeyError: If the key does not exist.
        """
        return ETagValue(f"{self.timestamp(key):.6f}")


    def oldest_keys(self, max_n: int|None=None) -> list[NonEmptySafeStrTuple]:
        """Return up to max_n oldest keys in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n: Maximum number of keys to return. If None,
                return all keys sorted by age (oldest first). Values <= 0
                yield an empty list. Defaults to None.

        Returns:
            The oldest keys, oldest first.
        """
        if max_n is None:
            # If we need all keys, sort them all by timestamp
            key_timestamp_pairs = list(self.keys_and_timestamps())
            key_timestamp_pairs.sort(key=lambda x: x[1])
            return [key for key,_ in key_timestamp_pairs]
        elif max_n <= 0:
            return []
        else:
            # Use heapq.nsmallest for efficient partial sorting without loading all keys into memory
            smallest_pairs = heapq.nsmallest(max_n,
                                             self.keys_and_timestamps(),
                                             key=lambda x: x[1])
            return [key for key,_ in smallest_pairs]


    def oldest_values(self, max_n: int | None = None) -> list[ValueType]:
        """Return up to max_n oldest values in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n: Maximum number of values to return. If None,
                return values for all keys sorted by age (oldest first). Values
                <= 0 yield an empty list.

        Returns:
            Values corresponding to the oldest keys.
        """
        return [self[k] for k in self.oldest_keys(max_n)]


    def newest_keys(self, max_n=None)  -> list[NonEmptySafeStrTuple]:
        """Return up to max_n newest keys in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n: Maximum number of keys to return. If None,
                return all keys sorted by age (newest first). Values <= 0
                yield an empty list. Defaults to None.

        Returns:
            The newest keys, newest first.
        """
        if max_n is None:
            # If we need all keys, sort them all by timestamp in reverse order
            key_timestamp_pairs = list(self.keys_and_timestamps())
            key_timestamp_pairs.sort(key=lambda x: x[1], reverse=True)
            return [key for key,_ in key_timestamp_pairs]
        elif max_n <= 0:
            return []
        else:
            # Use heapq.nlargest for efficient partial sorting without loading all keys into memory
            largest_pairs = heapq.nlargest(max_n,
                                           self.keys_and_timestamps(),
                                           key=lambda item: item[1])
            return [key for key,_ in largest_pairs]


    def newest_values(self, max_n: int | None = None) -> list[ValueType]:
        """Return up to max_n newest values in the dictionary.

        This method is absent in the original Python dict API.

        Args:
            max_n: Maximum number of values to return. If None,
                return values for all keys sorted by age (newest first). Values
                <= 0 yield an empty list.

        Returns:
            Values corresponding to the newest keys.
        """
        return [self[k] for k in self.newest_keys(max_n)]
