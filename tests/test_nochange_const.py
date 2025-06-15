
from src.persidict import NoChangeFlag, NoChange, NO_CHANGE

def test_NoChangeFlag():
    """Test if NoChangeFlag is a singleton.
    """
    assert NoChange is NoChangeFlag()
    assert NO_CHANGE is NoChangeFlag()
    NoChange_1 = NoChangeFlag()
    assert NoChange is NoChange_1