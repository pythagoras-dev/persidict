"""A singleton constant to indicate no change in a value.

When updating a value in a persistent dictionary,
use KEEP_CURRENT as the new value to indicate that
the existing value should remain unchanged.
"""
from typing import Any

from parameterizable import (
    ParameterizableClass
    , register_parameterizable_class)


class Joker(ParameterizableClass):
    _instances = {}

    def get_params(self) -> dict[str, Any]:
        return {}

    def __new__(cls):
        if cls not in Joker._instances:
            Joker._instances[cls] = super().__new__(cls)
        return Joker._instances[cls]


class KeepCurrentFlag(Joker):
    """A singleton constant to indicate no change in a value.

    When updating a value in a persistent dictionary,
    use KeepCurrent as the new value to indicate that
    the existing value (if any) should remain unchanged.
    """
    pass

class DeleteCurrentFlag(Joker):
    """A singleton constant to indicate that the current value should be deleted.

    When updating a value in a persistent dictionary,
    use DeleteCurrentFlag as the new value to indicate that
    the existing value (if any) should be removed from the dictionary.
    """
    pass

register_parameterizable_class(KeepCurrentFlag)
register_parameterizable_class(DeleteCurrentFlag)

KeepCurrent = KeepCurrentFlag()
KEEP_CURRENT = KeepCurrentFlag()

DeleteCurrent = DeleteCurrentFlag()
DELETE_CURRENT = DeleteCurrentFlag()
