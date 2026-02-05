"""Tests for error conditions, edge cases, and cross-backend consistency.

This file contains tests designed to uncover potential bugs through edge case
exploration and cross-backend consistency verification.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_HAS_NOT_CHANGED, ETAG_HAS_CHANGED
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_missing_key_raises_error(tmpdir, DictToTest, kwargs):
    """Verify etag() raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.etag("nonexistent")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_returns_string_type(tmpdir, DictToTest, kwargs):
    """Verify etag() returns a string (not bytes or other types)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    etag = d.etag("key1")

    assert isinstance(etag, str)
    assert not isinstance(etag, bytes)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_is_nonempty_string(tmpdir, DictToTest, kwargs):
    """Verify etag() returns a non-empty string."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    etag = d.etag("key1")

    assert len(etag) > 0


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_stable_without_modification(tmpdir, DictToTest, kwargs):
    """Verify multiple etag() calls return same value without modifications."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    etag1 = d.etag("key1")
    etag2 = d.etag("key1")
    etag3 = d.etag("key1")

    assert etag1 == etag2 == etag3


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_changes_after_modification(tmpdir, DictToTest, kwargs):
    """Verify etag() returns different value after value modification."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    etag_before = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes for all backends
    d["key1"] = "value2"
    etag_after = d.etag("key1")

    assert etag_before != etag_after


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_ops_with_empty_string_value(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work with empty string values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = ""
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", "updated", etag)

    assert result is not ETAG_HAS_CHANGED
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_ops_with_none_value(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work when value is None."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = None
    etag = d.etag("key1")

    assert d["key1"] is None

    result = d.set_item_if_etag_not_changed("key1", "updated", etag)

    assert result is not ETAG_HAS_CHANGED
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_equality_comparison_not_identity(tmpdir, DictToTest, kwargs):
    """Verify etag comparison uses equality (==), not identity (is).

    This tests that string interning doesn't break etag comparisons.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag1 = d.etag("key1")

    # Create a new string with same value via concatenation
    # to ensure it's a different object
    etag2 = "".join(list(etag1))

    # Verify they're equal but potentially different objects
    assert etag1 == etag2

    # The conditional operation should work with either
    result = d.set_item_if_etag_not_changed("key1", "updated", etag2)
    assert result is not ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_concurrent_modification_detection_simulation(tmpdir, DictToTest, kwargs):
    """Simulate concurrent modification and verify detection.

    This is a single-process simulation of what would happen with concurrent
    modifications. We get an etag, modify the value, then try conditional operation.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    stale_etag = d.etag("key1")

    # Simulate another process/thread modifying the value
    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified_by_other"

    # Our conditional operation should detect the change
    result = d.set_item_if_etag_not_changed("key1", "our_value", stale_etag)

    assert result is ETAG_HAS_CHANGED
    assert d["key1"] == "modified_by_other"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_set_item_if_etag_not_changed_success(tmpdir, DictToTest, kwargs):
    """Verify return type is str on successful set_item_if_etag_not_changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", "updated", etag)

    assert isinstance(result, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_set_item_if_etag_not_changed_failure(tmpdir, DictToTest, kwargs):
    """Verify return type is ETAG_HAS_CHANGED flag on failure."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if_etag_not_changed("key1", "updated", "wrong_etag")

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_delete_item_if_etag_not_changed_success(tmpdir, DictToTest, kwargs):
    """Verify return type is None on successful delete_item_if_etag_not_changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.delete_item_if_etag_not_changed("key1", etag)

    assert result is None


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_delete_item_if_etag_not_changed_failure(tmpdir, DictToTest, kwargs):
    """Verify return type is ETAG_HAS_CHANGED flag on failure."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.delete_item_if_etag_not_changed("key1", "wrong_etag")

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_discard_is_bool(tmpdir, DictToTest, kwargs):
    """Verify discard methods always return bool."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result_success = d.discard_item_if_etag_not_changed("key1", etag)
    result_missing = d.discard_item_if_etag_not_changed("key1", etag)  # Now missing

    assert isinstance(result_success, bool)
    assert isinstance(result_missing, bool)
    assert result_success is True
    assert result_missing is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_ops_with_complex_nested_value(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work with complex nested values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    complex_value = {
        "list": [1, 2, {"nested_key": "nested_value"}],
        "tuple": (1, 2, 3),
        "none": None,
        "bool": True,
        "int": 42,
        "float": 3.14
    }
    d["key1"] = complex_value
    etag = d.etag("key1")

    # Verify we can do conditional get
    result = d.get_item_if_etag_not_changed("key1", etag)
    assert result is not ETAG_HAS_CHANGED
    value, _ = result
    assert value == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_different_keys_have_independent_etags(tmpdir, DictToTest, kwargs):
    """Verify etags are independent between different keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    d["key2"] = "value1"  # Same value, different key

    etag1 = d.etag("key1")
    etag2 = d.etag("key2")

    # Etags may or may not be equal (depends on timestamp), but operations
    # on one key should not affect the other
    time.sleep(1.1)
    d["key1"] = "updated1"

    # key2's etag should be unchanged
    assert d.etag("key2") == etag2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_set_then_delete_in_sequence(tmpdir, DictToTest, kwargs):
    """Verify conditional set followed by conditional delete works correctly."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag1 = d.etag("key1")

    # Conditional set
    etag2 = d.set_item_if_etag_not_changed("key1", "updated", etag1)
    assert etag2 is not ETAG_HAS_CHANGED
    assert d["key1"] == "updated"

    # Conditional delete with new etag
    result = d.delete_item_if_etag_not_changed("key1", etag2)
    assert result is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_delete_then_recreate(tmpdir, DictToTest, kwargs):
    """Verify key can be recreated after conditional delete."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    # Delete
    d.delete_item_if_etag_not_changed("key1", etag)
    assert "key1" not in d

    # Recreate
    new_etag = d.set_item_get_etag("key1", "recreated")
    assert "key1" in d
    assert d["key1"] == "recreated"
    assert isinstance(new_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_status_flags_are_singleton_instances(tmpdir, DictToTest, kwargs):
    """Verify status flags can be compared using 'is' (singleton check)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result1 = d.set_item_if_etag_not_changed("key1", "new", "wrong_etag")
    result2 = d.set_item_if_etag_changed("key1", "new", current_etag)

    # Status flags should be the exact same singleton objects
    assert result1 is ETAG_HAS_CHANGED
    assert result2 is ETAG_HAS_NOT_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_preserves_value_type(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag preserves value types through serialization."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    # Test various types
    test_values = [
        42,
        3.14,
        True,
        False,
        None,
        "string",
        [1, 2, 3],
        {"key": "value"},
    ]

    for i, test_value in enumerate(test_values):
        key = f"key{i}"
        d[key] = test_value
        etag = d.etag(key)

        result = d.get_item_if_etag_not_changed(key, etag)
        assert result is not ETAG_HAS_CHANGED
        retrieved_value, _ = result
        assert retrieved_value == test_value
        assert type(retrieved_value) == type(test_value)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_long_key_tuples(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work with long hierarchical key tuples."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("level1", "level2", "level3", "level4", "level5")
    d[key] = "deep_value"
    etag = d.etag(key)

    result = d.set_item_if_etag_not_changed(key, "updated_deep", etag)

    assert result is not ETAG_HAS_CHANGED
    assert d[key] == "updated_deep"
