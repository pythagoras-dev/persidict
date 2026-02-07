from persidict import EmptyDict


def test_discard_on_empty_dict_returns_false():
    d = EmptyDict()
    assert d.discard("any_key") is False
    assert d.discard(("complex", "key")) is False
