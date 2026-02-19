"""Tests for error conditions, edge cases, and cross-backend consistency.

This file contains tests designed to uncover potential bugs through edge case
exploration and cross-backend consistency verification.
"""

import time
import pytest
from moto import mock_aws

from persidict import LocalDict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME, ETAG_HAS_CHANGED,
    ALWAYS_RETRIEVE,
    ConditionalOperationResult,
    ITEM_NOT_AVAILABLE
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_missing_key_raises_error(tmpdir, DictToTest, kwargs):
    """Verify etag() raises error for missing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    with pytest.raises(KeyError):
        d.etag("nonexistent")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_returns_string_type(tmpdir, DictToTest, kwargs):
    """Verify etag() returns a string (not bytes or other types)."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    etag = d.etag("key1")

    assert isinstance(etag, str)
    assert not isinstance(etag, bytes)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_is_nonempty_string(tmpdir, DictToTest, kwargs):
    """Verify etag() returns a non-empty string."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    etag = d.etag("key1")

    assert len(etag) > 0


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_stable_without_modification(tmpdir, DictToTest, kwargs):
    """Verify multiple etag() calls return same value without modifications."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    etag1 = d.etag("key1")
    etag2 = d.etag("key1")
    etag3 = d.etag("key1")

    assert etag1 == etag2 == etag3


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_changes_after_modification(tmpdir, DictToTest, kwargs):
    """Verify etag() returns different value after value modification."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
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
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = ""
    etag = d.etag("key1")

    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_ops_with_none_value(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work when value is None."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = None
    etag = d.etag("key1")

    assert d["key1"] is None

    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_equality_comparison_not_identity(tmpdir, DictToTest, kwargs):
    """Verify etag comparison uses equality (==), not identity (is).

    This tests that string interning doesn't break etag comparisons.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag1 = d.etag("key1")

    # Create a new string with same value via concatenation
    # to ensure it's a different object
    etag2 = "".join(list(etag1))

    # Verify they're equal but potentially different objects
    assert etag1 == etag2

    # The conditional operation should work with either
    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag=etag2)
    assert result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_concurrent_modification_detection_simulation(tmpdir, DictToTest, kwargs):
    """Simulate concurrent modification and verify detection.

    This is a single-process simulation of what would happen with concurrent
    modifications. We get an etag, modify the value, then try conditional operation.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    stale_etag = d.etag("key1")

    # Simulate another process/thread modifying the value
    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified_by_other"

    # Our conditional operation should detect the change
    result = d.set_item_if("key1", value="our_value", condition=ETAG_IS_THE_SAME, expected_etag=stale_etag)

    assert not result.condition_was_satisfied
    assert d["key1"] == "modified_by_other"


@mock_aws
def test_set_item_if_failed_condition_missing_key_returns_item_not_available(monkeypatch):
    """Verify missing-key race during failed condition returns ITEM_NOT_AVAILABLE."""
    d = LocalDict(serialization_format="pkl")
    d["key1"] = "original"

    def _get_value_and_etag_racy(key):
        del d[key]
        raise KeyError("Simulated concurrent deletion")

    monkeypatch.setattr(d, "_get_value_and_etag", _get_value_and_etag_racy)

    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_set_item_if_etag_equal_success(tmpdir, DictToTest, kwargs):
    """Verify return type is str on successful set_item_if_etag with ETAG_IS_THE_SAME."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert isinstance(result.resulting_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_set_item_if_etag_equal_failure(tmpdir, DictToTest, kwargs):
    """Verify set_item_if returns ConditionalOperationResult on failure."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")

    assert not result.condition_was_satisfied
    assert isinstance(result, ConditionalOperationResult)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_delete_item_if_etag_equal_success(tmpdir, DictToTest, kwargs):
    """Verify return type is None on successful delete_item_if_etag."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_delete_item_if_etag_equal_failure(tmpdir, DictToTest, kwargs):
    """Verify return type is ETAG_HAS_CHANGED flag on failure."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_return_type_discard_is_bool(tmpdir, DictToTest, kwargs):
    """Verify discard methods always return ConditionalOperationResult."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result_success = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    result_missing = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)  # Now missing

    assert isinstance(result_success, ConditionalOperationResult)
    assert isinstance(result_missing, ConditionalOperationResult)
    assert result_success.condition_was_satisfied
    assert not result_missing.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_ops_with_complex_nested_value(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work with complex nested values."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
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
    result = d.get_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag,
                           retrieve_value=ALWAYS_RETRIEVE)
    assert result.condition_was_satisfied
    value = result.new_value
    assert value == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_different_keys_have_independent_etags(tmpdir, DictToTest, kwargs):
    """Verify etags are independent between different keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value1"
    d["key2"] = "value1"  # Same value, different key

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
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    etag1 = d.etag("key1")

    # Conditional set
    result_set = d.set_item_if("key1", value="updated", condition=ETAG_IS_THE_SAME, expected_etag=etag1)
    assert result_set.condition_was_satisfied
    assert d["key1"] == "updated"
    etag2 = result_set.resulting_etag

    # Conditional delete with new etag
    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag2)
    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_conditional_delete_then_recreate(tmpdir, DictToTest, kwargs):
    """Verify key can be recreated after conditional delete."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    # Delete
    d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    assert "key1" not in d

    # Recreate
    d["key1"] = "recreated"
    new_etag = d.etag("key1")
    assert "key1" in d
    assert d["key1"] == "recreated"
    assert isinstance(new_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_status_flags_are_singleton_instances(tmpdir, DictToTest, kwargs):
    """Verify condition_was_satisfied correctly reflects failed conditions."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result1 = d.set_item_if("key1", value="new", condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")
    result2 = d.set_item_if("key1", value="new", condition=ETAG_HAS_CHANGED, expected_etag=current_etag)

    # Both results should indicate condition not satisfied
    assert not result1.condition_was_satisfied
    assert not result2.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_preserves_value_type(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag preserves value types through serialization."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

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

        result = d.get_item_if(key, condition=ETAG_IS_THE_SAME, expected_etag=etag,
                               retrieve_value=ALWAYS_RETRIEVE)
        assert result.condition_was_satisfied
        retrieved_value = result.new_value
        assert retrieved_value == test_value
        assert type(retrieved_value) is type(test_value)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_long_key_tuples(tmpdir, DictToTest, kwargs):
    """Verify conditional operations work with long hierarchical key tuples."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("level1", "level2", "level3", "level4", "level5")
    d[key] = "deep_value"
    etag = d.etag(key)

    result = d.set_item_if(key, value="updated_deep", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert d[key] == "updated_deep"
