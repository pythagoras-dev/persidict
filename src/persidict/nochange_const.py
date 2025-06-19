"""A singleton constant to indicate no change in a value.

When updating a value in a persistent dictionary,
use NO_CHANGE as the new value to indicate that
the existing value should remain unchanged.
"""
from typing import Any

from parameterizable import (
    ParameterizableClass
    , register_parameterizable_class)


class NoChangeFlag(ParameterizableClass):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_params(self) -> dict[str, Any]:
        return {}

register_parameterizable_class(NoChangeFlag)

NoChange = NoChangeFlag()
NO_CHANGE = NoChangeFlag()
