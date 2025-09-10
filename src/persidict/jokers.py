"""Special singleton markers used to modify values in PersiDict without data payload.

This module defines two singleton flags used as "joker" values when writing to
persistent dictionaries:

- KEEP_CURRENT: keep the current value unchanged.
- DELETE_CURRENT: delete the current value if it exists.

These flags are intended to be passed as the value part in dict-style
assignments (e.g., d[key] = KEEP_CURRENT) and are interpreted by PersiDict
implementations.

Examples:
    >>> from persidict.jokers import KEEP_CURRENT, DELETE_CURRENT
    >>> d[key] = KEEP_CURRENT  # Do not alter existing value
    >>> d[key] = DELETE_CURRENT  # Remove key if present
"""
from typing import Any

from parameterizable import (
    ParameterizableClass
    , register_parameterizable_class)


class Joker(ParameterizableClass):
    """Base class for singleton joker flags.

    Implements a per-subclass singleton pattern and integrates with the
    parameterizable framework. Subclasses represent value-less commands that
    alter persistence behavior when assigned to a key.

    Returns:
        Joker: The singleton instance for the subclass when instantiated.
    """
    _instances: dict[type, "Joker"] = {}

    def get_params(self) -> dict[str, Any]:
        """Return parameters for parameterizable API.

        Returns:
            dict[str, Any]: Always an empty dict for joker flags.
        """
        return {}

    def __new__(cls):
        """Create or return the singleton instance for the subclass."""
        if cls not in Joker._instances:
            Joker._instances[cls] = super().__new__(cls)
        return Joker._instances[cls]


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

register_parameterizable_class(KeepCurrentFlag)
register_parameterizable_class(DeleteCurrentFlag)

KeepCurrent = KeepCurrentFlag()
KEEP_CURRENT = KeepCurrentFlag()

DeleteCurrent = DeleteCurrentFlag()
DELETE_CURRENT = DeleteCurrentFlag()
