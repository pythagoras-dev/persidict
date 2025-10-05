"""Special singleton markers used to modify values in PersiDict without data payload.

This module defines two singleton flags used as "joker" values when writing to
persistent dictionaries:

- KEEP_CURRENT: keep the current value unchanged.
- DELETE_CURRENT: delete the current value if it exists.

These flags are intended to be passed as the value part in dict-style
assignments (e.g., d[key] = KEEP_CURRENT) and are interpreted by PersiDict
implementations.

Examples:
    >>> from persidict.singletons import KEEP_CURRENT, DELETE_CURRENT
    >>> d[key] = KEEP_CURRENT  # Do not alter existing value
    >>> d[key] = DELETE_CURRENT  # Remove key if present
"""
from typing import Any

from parameterizable import ParameterizableClass
    # , register_parameterizable_class)


class Singleton(ParameterizableClass):
    """Base class for singleton classes.

    This class implements a singleton pattern where each subclass maintains
    exactly one instance that is returned on every instantiation.
    """
    _instances: dict[type, "Singleton"] = {}

    def get_params(self) -> dict[str, Any]:
        """Return parameters for parameterizable API.

        Returns:
            dict[str, Any]: Always an empty dict for joker flags.
        """
        return {}

    def __new__(cls):
        """Create or return the singleton instance for the subclass.
        
        Args:
            cls: The class for which to create or retrieve the singleton instance.
            
        Returns:
            Joker: The singleton instance for the specified class.
        """
        if cls not in Singleton._instances:
            Singleton._instances[cls] = super().__new__(cls)
        return Singleton._instances[cls]


class Joker(Singleton):
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


class StatusFlag(Singleton):
    """Base class for process status flags.

    Subclasses represent status flags that can be used to control
    processing flow in various contexts.
    """
    pass

class ETagHasNotChangedFlag(StatusFlag):
    """Flag indicating that an ETag has not changed.

    Usage:
        This flag can be used in contexts where a notification is needed
        to indicate that an ETag (entity tag) has not changed, typically in
        web or caching scenarios.
    """

    pass


class ContinueNormalExecutionFlag(StatusFlag):
    """Flag indicating to continue normal execution without special handling.

    Usage:
        This flag can be used in contexts where a notification is needed
        to indicate that normal processing should proceed without alteration.

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
        finished successfully and nore further action is needed.

    Note:
        This is a singleton class; constructing it repeatedly returns the same
        instance.
    """
    pass


# register_parameterizable_class(KeepCurrentFlag)
# register_parameterizable_class(DeleteCurrentFlag)
# register_parameterizable_class(ContinueNormalExecutionFlag)
# register_parameterizable_class(ExecutionIsCompleteFlag)
# register_parameterizable_class(ETagHasNotChangedFlag)

_KeepCurrent = KeepCurrentFlag()
KEEP_CURRENT = KeepCurrentFlag()
"""Flag indicating that the current value should be kept unchanged.

This flag can be assigned to a key in a PersiDict to indicate that any existing
value for that key should not be modified during an update operation.

If assigned to a key that does not exist, the operation will succeed without
eny change.

Example:
    >>> d = PersiDict()
    >>> d['key'] = 'value'
    >>> d['key'] = KEEP_CURRENT  # Keeps 'value' unchanged
"""


_DeleteCurrent = DeleteCurrentFlag()
DELETE_CURRENT = DeleteCurrentFlag()
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
CONTINUE_NORMAL_EXECUTION = ContinueNormalExecutionFlag()
"""Flag indicating that normal execution should continue.

This flag can be used in process flow control contexts to signal that normal
execution should proceed without any special handling or alterations.

When this flag is returned from a processing step, it indicates that the
operation completed successfully and the next step in the normal execution
flow should be performed.

Example:
    >>> if pre_process_input(data) is CONTINUE_NORMAL_EXECUTION:
    ...     # Continue with next step
    ...     perform_next_step()
"""

_ExecutionIsComplete = ExecutionIsCompleteFlag()
EXECUTION_IS_COMPLETE = ExecutionIsCompleteFlag()
"""Flag indicating that execution is complete, no further processing is needed.

This flag can be used in process flow control contexts to signal that all necessary
processing has been completed successfully and no additional steps are required.

When this flag is returned from a processing step, it indicates that the
operation completed successfully and no further processing should be performed.

Example:
    >>> if pre_process_input(data) is EXECUTION_IS_COMPLETE:
    ...     # Skip remaining steps
    ...     return result
"""

_ETagHasNotChanged = ETagHasNotChangedFlag()
ETAG_HAS_NOT_CHANGED = ETagHasNotChangedFlag()
"""Flag indicating that an ETag value has not changed.

This flag can be used in contexts where a notification is needed to indicate
that an ETag (entity tag) comparison shows no changes.

When this flag is returned from a processing step, it indicates that the
resource's ETag matches and no content updates are necessary.

Example:
    >>> if check_resource_etag(url) is ETAG_HAS_NOT_CHANGED:
    ...     # Skip resource update
    ...     return cached_content
"""
