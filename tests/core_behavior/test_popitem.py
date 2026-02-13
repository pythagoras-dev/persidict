"""Tests for the PersiDict.popitem override.

popitem removes and returns an arbitrary (key, value) pair, delegating
to pop (which uses transform_item) for race-safe read-then-delete.
"""

import pytest
from moto import mock_aws

from persidict import FileDirDict, LocalDict
from tests.data_for_mutable_tests import mutable_tests, make_test_dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_popitem_returns_key_value_pair(tmpdir, DictToTest, kwargs):
    """popitem returns a (key, value) tuple and removes the item."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["a"] = 1
    d["b"] = 2

    key, value = d.popitem()
    assert key in (("a",), ("b",))
    assert value in (1, 2)
    assert key not in d
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_popitem_drains_all_items(tmpdir, DictToTest, kwargs):
    """Repeated popitem calls drain the dictionary completely."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    items = {("x",): 10, ("y",): 20, ("z",): 30}
    for k, v in items.items():
        d[k] = v

    popped = {}
    for _ in range(3):
        k, v = d.popitem()
        popped[k] = v

    assert len(d) == 0
    assert popped == items


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_popitem_on_empty_raises_key_error(tmpdir, DictToTest, kwargs):
    """popitem on an empty dict raises KeyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    with pytest.raises(KeyError):
        d.popitem()


def test_popitem_no_redundant_contains_file_dir(tmp_path):
    """FileDirDict.popitem must not call __contains__ during delete."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "value"

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        key, value = d.popitem()
    finally:
        type(d).__contains__ = original_contains

    assert value == "value"
    assert key not in d
    assert contains_calls == 0, (
        f"popitem performed {contains_calls} __contains__ call(s); "
        "expected 0 (transform_item + _remove_item should not re-check)")


def test_popitem_no_redundant_contains_local(tmp_path):
    """LocalDict.popitem must not call __contains__ during delete."""
    d = LocalDict(serialization_format="json")
    d["k"] = "value"

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        key, value = d.popitem()
    finally:
        type(d).__contains__ = original_contains

    assert value == "value"
    assert key not in d
    assert contains_calls == 0, (
        f"popitem performed {contains_calls} __contains__ call(s); "
        "expected 0 (transform_item + _remove_item should not re-check)")
