"""Comprehensive tests for the get_subdict() hierarchical sub-dictionary feature."""

import time
import pytest
from moto import mock_aws

from persidict import PersiDict
from persidict.safe_str_tuple import SafeStrTuple

from tests.data_for_mutable_tests import mutable_tests, make_test_dict

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_returns_same_type(tmpdir, DictToTest, kwargs):
    """Verify get_subdict returns the same type as the parent dict."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("prefix", "key1")] = "value1"

    sub = d.get_subdict("prefix")

    # Subdict must be a PersiDict of the same concrete type
    assert isinstance(sub, PersiDict)
    assert type(sub).__name__ == type(d).__name__


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_length_reflects_prefix_items(tmpdir, DictToTest, kwargs):
    """Verify len() on subdict only counts items with matching prefix."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("a", "1")] = 10
    d[("a", "2")] = 20
    d[("b", "1")] = 30
    d[("c", "1")] = 40

    sub_a = d.get_subdict("a")
    sub_b = d.get_subdict("b")
    sub_c = d.get_subdict("c")
    sub_x = d.get_subdict("x")

    assert len(sub_a) == 2
    assert len(sub_b) == 1
    assert len(sub_c) == 1
    assert len(sub_x) == 0


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_write_propagates_to_parent(tmpdir, DictToTest, kwargs):
    """Verify writes through subdict are visible in parent."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("prefix", "existing")] = "original"

    sub = d.get_subdict("prefix")
    sub[("new_key",)] = "new_value"

    assert d[("prefix", "new_key")] == "new_value"
    assert len(d) == 2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_parent_write_visible_in_subdict(tmpdir, DictToTest, kwargs):
    """Verify writes to parent are visible through subdict."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    sub = d.get_subdict("prefix")

    d[("prefix", "key1")] = "value1"

    assert sub[("key1",)] == "value1"
    assert len(sub) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_delete_propagates_bidirectional(tmpdir, DictToTest, kwargs):
    """Verify deletions propagate both from subdict to parent and vice versa."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("prefix", "key1")] = "value1"
    d[("prefix", "key2")] = "value2"
    d[("prefix", "key3")] = "value3"

    sub = d.get_subdict("prefix")

    # Delete via subdict
    del sub[("key1",)]
    assert ("prefix", "key1") not in d
    assert len(d) == 2

    # Delete via parent
    del d[("prefix", "key2")]
    assert ("key2",) not in sub
    assert len(sub) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_nested_prefixes(tmpdir, DictToTest, kwargs):
    """Verify multi-level prefix like get_subdict(('a', 'b')) works correctly."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("a", "b", "c")] = 1
    d[("a", "b", "d")] = 2
    d[("a", "x", "y")] = 3

    sub_ab = d.get_subdict(("a", "b"))

    assert len(sub_ab) == 2
    assert sub_ab[("c",)] == 1
    assert sub_ab[("d",)] == 2
    assert ("y",) not in sub_ab


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_nonexistent_prefix_returns_empty(tmpdir, DictToTest, kwargs):
    """Verify get_subdict with nonexistent prefix returns empty dict, not error."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("existing", "key")] = "value"

    sub = d.get_subdict("nonexistent")

    assert len(sub) == 0
    assert list(sub.keys()) == []
    assert list(sub.values()) == []
    assert list(sub.items()) == []


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_iteration_methods(tmpdir, DictToTest, kwargs):
    """Verify keys(), values(), items() work correctly on subdict."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("prefix", "a")] = 1
    d[("prefix", "b")] = 2
    d[("other", "c")] = 3

    sub = d.get_subdict("prefix")

    keys = list(sub.keys())
    values = sorted(sub.values())
    items = list(sub.items())

    assert len(keys) == 2
    assert all(isinstance(k, SafeStrTuple) for k in keys)
    assert set(tuple(k) for k in keys) == {("a",), ("b",)}
    assert values == [1, 2]
    assert len(items) == 2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_timestamp_behavior(tmpdir, DictToTest, kwargs):
    """Verify timestamps are accessible through subdict."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("prefix", "key1")] = "value1"
    # Use 1.1s sleep for backends with 1-second resolution (mocked S3)
    time.sleep(1.1)
    d[("prefix", "key2")] = "value2"

    sub = d.get_subdict("prefix")

    ts1 = sub.timestamp(("key1",))
    ts2 = sub.timestamp(("key2",))

    assert isinstance(ts1, float)
    assert isinstance(ts2, float)
    assert ts2 > ts1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_subdict_with_complex_keys(tmpdir, DictToTest, kwargs):
    """Verify subdict works with complex multi-segment keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d[("root", "level1", "level2", "leaf")] = "deep_value"

    sub_root = d.get_subdict("root")
    sub_level1 = sub_root.get_subdict("level1")

    assert sub_root[("level1", "level2", "leaf")] == "deep_value"
    assert sub_level1[("level2", "leaf")] == "deep_value"
    assert len(sub_level1) == 1
