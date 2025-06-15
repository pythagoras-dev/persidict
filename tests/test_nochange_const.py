
from src.persidict import NoChange_Class, NoChange, NO_CHANGE

def test_NoChangeClass():
    """Test if NoChange_Class is a singleton.
    """
    assert NoChange is NoChange_Class()
    assert NO_CHANGE is NoChange_Class()
    NoChange_1 = NoChange_Class()
    assert NoChange is NoChange_1