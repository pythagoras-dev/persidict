"""Tests for the update() method on PersiDict implementations.

The update() method is inherited from MutableMapping and should behave
like dict.update(), supporting:
- Another mapping (dict or PersiDict)
- An iterable of key-value pairs
- Keyword arguments
"""

import pytest
from moto import mock_aws

from persidict import FileDirDict, LocalDict, SafeStrTuple

from .data_for_mutable_tests import mutable_tests


# =============================================================================
# Basic update() functionality
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_dict(tmpdir, DictToTest, kwargs):
    """update() with a standard dict adds all key-value pairs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    source = {
        ("key", "one"): "value1",
        ("key", "two"): "value2",
        ("key", "three"): "value3",
    }

    d.update(source)

    assert len(d) == 3
    assert d[("key", "one")] == "value1"
    assert d[("key", "two")] == "value2"
    assert d[("key", "three")] == "value3"

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_iterable_of_pairs(tmpdir, DictToTest, kwargs):
    """update() with an iterable of (key, value) pairs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    pairs = [
        (("a",), 1),
        (("b",), 2),
        (("c",), 3),
    ]

    d.update(pairs)

    assert len(d) == 3
    assert d[("a",)] == 1
    assert d[("b",)] == 2
    assert d[("c",)] == 3

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_overwrites_existing_keys(tmpdir, DictToTest, kwargs):
    """update() overwrites values for existing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    d[("existing",)] = "old_value"
    assert d[("existing",)] == "old_value"

    d.update({("existing",): "new_value"})
    assert d[("existing",)] == "new_value"

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_another_persidict(tmpdir, DictToTest, kwargs):
    """update() can use another PersiDict as the source."""
    d1 = DictToTest(base_dir=tmpdir, **kwargs)
    d1.clear()
    d1[("from", "d1")] = "d1_value"

    # Create a second dict with different base_dir to avoid conflicts
    d2 = LocalDict(serialization_format="pkl")
    d2[("from", "d2")] = "d2_value"
    d2[("another", "key")] = 42

    d1.update(d2)

    assert d1[("from", "d1")] == "d1_value"
    assert d1[("from", "d2")] == "d2_value"
    assert d1[("another", "key")] == 42
    assert len(d1) == 3

    d1.clear()
    d2.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_empty_source(tmpdir, DictToTest, kwargs):
    """update() with empty source leaves dict unchanged."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()
    d[("pre", "existing")] = "value"

    d.update({})
    d.update([])

    assert len(d) == 1
    assert d[("pre", "existing")] == "value"

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_on_empty_dict(tmpdir, DictToTest, kwargs):
    """update() on an empty dict adds all items."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    assert len(d) == 0

    d.update({("new",): "item"})

    assert len(d) == 1
    assert d[("new",)] == "item"

    d.clear()


# =============================================================================
# update() with SafeStrTuple keys
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_safe_str_tuple_keys(tmpdir, DictToTest, kwargs):
    """update() works correctly with SafeStrTuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    key1 = SafeStrTuple("safe", "key", "one")
    key2 = SafeStrTuple("safe", "key", "two")

    d.update({key1: "val1", key2: "val2"})

    assert d[key1] == "val1"
    assert d[key2] == "val2"
    # Also accessible via tuple
    assert d[("safe", "key", "one")] == "val1"

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_mixed_key_formats(tmpdir, DictToTest, kwargs):
    """update() handles mixed key formats (tuple and SafeStrTuple)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    d.update({
        ("tuple", "key"): "from_tuple",
        SafeStrTuple("safe", "key"): "from_safe",
    })

    assert len(d) == 2
    assert d[("tuple", "key")] == "from_tuple"
    assert d[SafeStrTuple("safe", "key")] == "from_safe"

    d.clear()


# =============================================================================
# update() with various value types
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_complex_values(tmpdir, DictToTest, kwargs):
    """update() handles various value types correctly."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    complex_data = {
        ("str",): "string_value",
        ("int",): 42,
        ("float",): 3.14,
        ("list",): [1, 2, 3],
        ("dict",): {"nested": "dict"},
        ("none",): None,
    }

    d.update(complex_data)

    assert d[("str",)] == "string_value"
    assert d[("int",)] == 42
    assert d[("float",)] == 3.14
    assert d[("list",)] == [1, 2, 3]
    assert d[("dict",)] == {"nested": "dict"}
    assert d[("none",)] is None

    d.clear()


# =============================================================================
# Multiple update() calls
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_multiple_updates(tmpdir, DictToTest, kwargs):
    """Multiple update() calls accumulate and overwrite correctly."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    d.update({("a",): 1, ("b",): 2})
    assert len(d) == 2

    d.update({("c",): 3, ("d",): 4})
    assert len(d) == 4

    # Overwrite some
    d.update({("a",): 10, ("c",): 30})
    assert len(d) == 4
    assert d[("a",)] == 10
    assert d[("b",)] == 2
    assert d[("c",)] == 30
    assert d[("d",)] == 4

    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_returns_none(tmpdir, DictToTest, kwargs):
    """update() returns None, like dict.update()."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    result = d.update({("key",): "value"})
    assert result is None

    d.clear()


# =============================================================================
# update() with generator/iterator
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_with_generator(tmpdir, DictToTest, kwargs):
    """update() works with a generator of key-value pairs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    def pair_generator():
        for i in range(5):
            yield (f"gen_{i}",), i * 10

    d.update(pair_generator())

    assert len(d) == 5
    for i in range(5):
        assert d[(f"gen_{i}",)] == i * 10

    d.clear()


# =============================================================================
# Comparison with standard dict behavior
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_update_matches_dict_behavior(tmpdir, DictToTest, kwargs):
    """PersiDict.update() behaves like dict.update()."""
    pd = DictToTest(base_dir=tmpdir, **kwargs)
    pd.clear()
    model = {}

    # Same operations on both
    data1 = {("x",): 1, ("y",): 2}
    pd.update(data1)
    model.update({k: v for k, v in data1.items()})

    data2 = [(("z",), 3), (("w",), 4)]
    pd.update(data2)
    model.update(data2)

    data3 = {("x",): 100}  # overwrite
    pd.update(data3)
    model.update({k: v for k, v in data3.items()})

    # Verify same state
    assert len(pd) == len(model)
    for k, v in model.items():
        assert pd[k] == v

    pd.clear()
