import pytest

from persidict import LocalDict


def test_discard_and_clear():
    ld = LocalDict()
    ld[("x",)] = 10
    assert ld.discard(("x",)) is True
    assert ld.discard(("x",)) is False

    # clear removes everything
    for i in range(3):
        ld[("k", str(i))] = i
    assert len(ld) == 3
    ld.clear()
    assert len(ld) == 0
    # clearing an empty dict is fine
    ld.clear()


def test_discard_immutable_raises():
    ld = LocalDict(append_only=True)
    k = ("root", "leaf")
    ld[k] = 5
    with pytest.raises(KeyError):
        ld.discard(k)
