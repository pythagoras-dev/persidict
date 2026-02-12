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

Result dataclasses:
    - OperationResult: result of transform_item.
    - ConditionalOperationResult: result of conditional _if methods.
"""
from dataclasses import dataclass
from typing import Any, Callable, Final, NewType, TypeAlias

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

    Returned in ``new_value`` when ``always_retrieve_value=False`` and
    the value was already known to the caller or retrieval was skipped.

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

_KeepCurrent = KeepCurrentFlag()
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


_DeleteCurrent = DeleteCurrentFlag()
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


_ContinueNormalExecution = ContinueNormalExecutionFlag()
CONTINUE_NORMAL_EXECUTION: Final[ContinueNormalExecutionFlag] = ContinueNormalExecutionFlag()
"""Flag indicating that normal execution should continue."""

_ExecutionIsComplete = ExecutionIsCompleteFlag()
EXECUTION_IS_COMPLETE: Final[ExecutionIsCompleteFlag] = ExecutionIsCompleteFlag()
"""Flag indicating that execution is complete, no further processing is needed."""

_ItemNotAvailable = ItemNotAvailableFlag()
ITEM_NOT_AVAILABLE: Final[ItemNotAvailableFlag] = ItemNotAvailableFlag()
"""Sentinel: the item is not present in the dict.

Used uniformly for absent keys in all contexts: etag parameters/fields,
value fields, and transformer input.
"""

_ValueNotRetrieved = ValueNotRetrievedFlag()
VALUE_NOT_RETRIEVED: Final[ValueNotRetrievedFlag] = ValueNotRetrievedFlag()
"""Sentinel: the value exists but was not retrieved.

Returned when ``always_retrieve_value=False`` and the value was already
known to the caller or retrieval was skipped.
"""

_AnyETag = AnyETagFlag()
ANY_ETAG: Final[AnyETagFlag] = AnyETagFlag()
"""Condition: always satisfied regardless of etag values."""

_ETagIsTheSame = ETagIsTheSameFlag()
ETAG_IS_THE_SAME: Final[ETagIsTheSameFlag] = ETagIsTheSameFlag()
"""Condition: expected and actual etags must match."""

_ETagHasChanged = ETagHasChangedFlag()
ETAG_HAS_CHANGED: Final[ETagHasChangedFlag] = ETagHasChangedFlag()
"""Condition: expected and actual etags must differ."""

# --- Type aliases ---

ValueType = Any
"""Type alias for values stored in a PersiDict."""

ETagIfExists: TypeAlias = ETagValue | ItemNotAvailableFlag
"""ETag value or ITEM_NOT_AVAILABLE if the key is absent."""

ValueIfExists: TypeAlias = ValueType | ItemNotAvailableFlag
"""Value or ITEM_NOT_AVAILABLE if the key is absent."""

ValueInResult: TypeAlias = ValueType | ItemNotAvailableFlag | ValueNotRetrievedFlag
"""Value, ITEM_NOT_AVAILABLE, or VALUE_NOT_RETRIEVED in operation results."""

TransformingFunction = Callable[
    [ValueIfExists], ValueType | KeepCurrentFlag | DeleteCurrentFlag]
"""Callable that takes the current value (or ITEM_NOT_AVAILABLE) and returns
a new value, DELETE_CURRENT, or KEEP_CURRENT."""


# --- Result dataclasses ---

@dataclass(frozen=True)
class OperationResult:
    """Result of an unconditional mutating operation (transform_item)."""
    resulting_etag: ETagIfExists
    new_value: ValueIfExists


@dataclass(frozen=True)
class ConditionalOperationResult:
    """Result of a conditional operation guarded by an ETag check."""
    condition_was_satisfied: bool
    requested_condition: ETagConditionFlag
    actual_etag: ETagIfExists
    resulting_etag: ETagIfExists
    new_value: ValueInResult

    @property
    def value_was_mutated(self) -> bool:
        """Whether the operation changed the stored value."""
        return self.resulting_etag != self.actual_etag
