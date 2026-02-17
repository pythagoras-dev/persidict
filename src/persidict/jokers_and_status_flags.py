"""Special singleton markers and result types for PersiDict operations.

This module defines singleton flags used as "joker" values when writing to
persistent dictionaries, ETag condition flags for conditional operations,
and structured result dataclasses.

Joker flags:
    - KEEP_CURRENT: keep the current value unchanged.
    - DELETE_CURRENT: delete the current value if it exists.

Sentinel flags:
    - ITEM_NOT_AVAILABLE: the item is not present in the dict.
    - VALUE_NOT_RETRIEVED: the value exists but was not retrieved.

ETag condition flags:
    - ANY_ETAG: condition always satisfied.
    - ETAG_IS_THE_SAME: condition requires etags to match.
    - ETAG_HAS_CHANGED: condition requires etags to differ.

Protocols:
    - TransformingFunction: generic callback protocol for transform_item.

Result dataclasses:
    - OperationResult: result of transform_item.
    - ConditionalOperationResult: result of conditional _if methods.
"""
from dataclasses import dataclass
from typing import (Final, Generic, NewType, Protocol,
                     TypeAlias, TypeVar, runtime_checkable)

from mixinforge import SingletonMixin


class Joker(SingletonMixin):
    """Base class for joker flags.

    Subclasses represent value-less commands that
    alter persistence behavior when assigned to a key.
    """
    pass


class KeepCurrentFlag(Joker):
    """Flag instructing PersiDict to keep the current value unchanged.

    Usage:
        Assign this flag instead of a real value to indicate that an existing
        value should not be modified.

    Examples:
        >>> d[key] = KEEP_CURRENT

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass

class DeleteCurrentFlag(Joker):
    """Flag instructing PersiDict to delete the current value for a key.

    Usage:
        Assign this flag instead of a real value to remove the key if it
        exists. If the key is absent, implementations will typically no-op.

    Examples:
        >>> d[key] = DELETE_CURRENT

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass


class StatusFlag(SingletonMixin):
    """Base class for process status flags.

    Subclasses represent status flags that can be used to control
    processing flow in various contexts.
    """
    pass


class ItemNotAvailableFlag(SingletonMixin):
    """Sentinel indicating that the item is not present in the dict.

    Used uniformly for absent keys across all contexts:
    - As ``expected_etag``: "I believe the key is absent."
    - As ``actual_etag``: "the key was absent at check time."
    - As ``resulting_etag``: "the key is absent after the operation."
    - As ``new_value``: "no value to return."
    - As transformer input: "transforming from absence (creating new)."

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass


class ValueNotRetrievedFlag(SingletonMixin):
    """Sentinel indicating the value exists but was not retrieved.

    Returned in ``new_value`` when ``retrieve_value=NEVER_RETRIEVE`` or
    when ``retrieve_value=IF_ETAG_CHANGED`` and the ETag has not changed.

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass


ETagValue = NewType("ETagValue", str)
"""Type for ETag string values."""


class ETagConditionFlag(SingletonMixin):
    """Base class for ETag condition selectors."""
    pass


class AnyETagFlag(ETagConditionFlag):
    """Condition that is always satisfied regardless of etag values."""
    pass


class ETagIsTheSameFlag(ETagConditionFlag):
    """Condition requiring expected and actual etags to match."""
    pass


class ETagHasChangedFlag(ETagConditionFlag):
    """Condition requiring expected and actual etags to differ."""
    pass


class RetrieveValueFlag(SingletonMixin):
    """Base class for value retrieval strategy flags.

    Subclasses control whether and when the actual value is fetched
    in conditional operations.
    """
    pass


class AlwaysRetrieveFlag(RetrieveValueFlag):
    """Always retrieve the value in conditional operations."""
    pass


class NeverRetrieveFlag(RetrieveValueFlag):
    """Never retrieve the value; always return VALUE_NOT_RETRIEVED."""
    pass


class IfETagChangedRetrieveFlag(RetrieveValueFlag):
    """Retrieve the value only if the actual ETag differs from expected."""
    pass


class ContinueNormalExecutionFlag(StatusFlag):
    """Flag indicating to continue normal execution without special handling.

    Usage:
        This flag can be used in contexts where a notification is needed
        to indicate that normal processing should continue without alteration.

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass

class ExecutionIsCompleteFlag(StatusFlag):
    """Flag indicating no more processing is required.

    Usage:
        This flag can be used in contexts where a notification is needed
        to indicate that all necessary processing steps were
        finished successfully and no further action is needed.

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass

# --- Singleton constant instances ---

KEEP_CURRENT: Final[KeepCurrentFlag] = KeepCurrentFlag()
"""Flag indicating that the current value should be kept unchanged.

This flag can be assigned to a key in a PersiDict to indicate that any existing
value for that key should not be modified during an update operation.

If assigned to a key that does not exist, the operation will succeed without
any change.

Example:
    >>> d = PersiDict()
    >>> d['key'] = 'value'
    >>> d['key'] = KEEP_CURRENT  # Keeps 'value' unchanged
"""


DELETE_CURRENT: Final[DeleteCurrentFlag] = DeleteCurrentFlag()
"""Flag indicating that the current value should be deleted.

This flag can be assigned to a key in a PersiDict to indicate that any existing
value for that key should be deleted during an update operation.

If assigned to a key that does not exist, the operation will succeed without
any change.

Example:
    >>> d = PersiDict()
    >>> d['key'] = 'value'
    >>> d['key'] = DELETE_CURRENT  # same as d.discard('key')
"""


CONTINUE_NORMAL_EXECUTION: Final[ContinueNormalExecutionFlag] = ContinueNormalExecutionFlag()
"""Flag indicating that normal execution should continue."""

EXECUTION_IS_COMPLETE: Final[ExecutionIsCompleteFlag] = ExecutionIsCompleteFlag()
"""Flag indicating that execution is complete, no further processing is needed."""

ITEM_NOT_AVAILABLE: Final[ItemNotAvailableFlag] = ItemNotAvailableFlag()
"""Sentinel: the item is not present in the dict.

Used uniformly for absent keys in all contexts: etag parameters/fields,
value fields, and transformer input.
"""

VALUE_NOT_RETRIEVED: Final[ValueNotRetrievedFlag] = ValueNotRetrievedFlag()
"""Sentinel: the value exists but was not retrieved.

Returned when ``retrieve_value=NEVER_RETRIEVE`` or when
``retrieve_value=IF_ETAG_CHANGED`` and the ETag has not changed.
"""

ALWAYS_RETRIEVE: Final[AlwaysRetrieveFlag] = AlwaysRetrieveFlag()
"""Retrieve the value unconditionally in conditional operations."""

NEVER_RETRIEVE: Final[NeverRetrieveFlag] = NeverRetrieveFlag()
"""Never retrieve the value; always return VALUE_NOT_RETRIEVED."""

IF_ETAG_CHANGED: Final[IfETagChangedRetrieveFlag] = IfETagChangedRetrieveFlag()
"""Retrieve the value only if the actual ETag differs from expected."""

ANY_ETAG: Final[AnyETagFlag] = AnyETagFlag()
"""Condition: always satisfied regardless of etag values."""

ETAG_IS_THE_SAME: Final[ETagIsTheSameFlag] = ETagIsTheSameFlag()
"""Condition: expected and actual etags must match."""

ETAG_HAS_CHANGED: Final[ETagHasChangedFlag] = ETagHasChangedFlag()
"""Condition: expected and actual etags must differ."""

# --- Type aliases ---

ValueType = TypeVar('ValueType')
"""Generic type variable for values stored in a PersiDict."""

ETagIfExists: TypeAlias = ETagValue | ItemNotAvailableFlag
"""ETag value or ITEM_NOT_AVAILABLE if the key is absent."""

@runtime_checkable
class TransformingFunction(Protocol[ValueType]):
    """Protocol for transform_item callback functions.

    A TransformingFunction receives the current value (or
    ITEM_NOT_AVAILABLE when the key is absent) and returns a new value,
    KEEP_CURRENT, or DELETE_CURRENT.

    Generic over ValueType so that ``transform_item`` on a
    ``PersiDict[int]`` expects a transformer whose input and output are
    both typed in terms of ``int``.
    """

    def __call__(
            self, current: ValueType | ItemNotAvailableFlag, /
    ) -> ValueType | KeepCurrentFlag | DeleteCurrentFlag: ...


# --- Result dataclasses ---

@dataclass(frozen=True)
class OperationResult(Generic[ValueType]):
    """Result of an unconditional mutating operation (transform_item).

    Attributes:
        resulting_etag: ETag after the operation, or ITEM_NOT_AVAILABLE
            if the key is absent.
        new_value: The value after the operation, or ITEM_NOT_AVAILABLE
            if the key is absent.
    """
    resulting_etag: ETagIfExists
    new_value: ValueType | ItemNotAvailableFlag


@dataclass(frozen=True)
class ConditionalOperationResult(Generic[ValueType]):
    """Result of a conditional operation guarded by an ETag check.

    Attributes:
        condition_was_satisfied: Whether the ETag condition was met.
        actual_etag: ETag of the key before the operation, or
            ITEM_NOT_AVAILABLE if the key was absent.
        resulting_etag: ETag after the operation, or ITEM_NOT_AVAILABLE
            if the key is absent.
        new_value: The value after the operation. May be
            ITEM_NOT_AVAILABLE (key absent) or VALUE_NOT_RETRIEVED
            (value fetch was skipped).
    """
    condition_was_satisfied: bool
    actual_etag: ETagIfExists
    resulting_etag: ETagIfExists
    new_value: ValueType | ItemNotAvailableFlag | ValueNotRetrievedFlag

    @property
    def value_was_mutated(self) -> bool:
        """Whether the operation changed the stored value."""
        return self.resulting_etag != self.actual_etag
