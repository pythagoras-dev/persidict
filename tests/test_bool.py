from persidict import LocalDict

def test_bool_empty_dict():
    """Empty dict should be falsy."""
    d = LocalDict()
    assert not d
    assert bool(d) is False

def test_bool_non_empty_dict():
    """Non-empty dict should be truthy."""
    d = LocalDict()
    d["key"] = "value"
    assert d
    assert bool(d) is True

def test_bool_after_clear():
    """Dict should be falsy after clearing."""
    d = LocalDict()
    d["key"] = "value"
    d.clear()
    assert not d