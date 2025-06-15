"""A singleton constant to indicate no change in a value.

When updating a val ue in a persistent dictionary,
use NO_CHANGE as the new value to indicate that
the existing value should remain unchanged.
"""

class NoChangeFlag:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

NoChange = NoChangeFlag()
NO_CHANGE = NoChangeFlag()
