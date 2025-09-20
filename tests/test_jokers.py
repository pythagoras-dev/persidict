
from persidict.singletons import (
    KeepCurrentFlag, _KeepCurrent, KEEP_CURRENT
    , DeleteCurrentFlag, _DeleteCurrent, DELETE_CURRENT)

def test_Jokers():
    """Test if KeepCurrentFlag is a singleton.
    """
    assert _KeepCurrent is KeepCurrentFlag()
    assert KEEP_CURRENT is KeepCurrentFlag()
    KEEP_CURRENT_1 = KeepCurrentFlag()
    assert KEEP_CURRENT is KEEP_CURRENT_1

    assert _DeleteCurrent is DeleteCurrentFlag()
    assert DELETE_CURRENT is DeleteCurrentFlag()
    DELETE_CURRENT_1 = DeleteCurrentFlag()
    assert DELETE_CURRENT is DELETE_CURRENT_1

    assert KEEP_CURRENT is not DELETE_CURRENT