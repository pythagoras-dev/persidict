"""Tests for KEEP_CURRENT and DELETE_CURRENT joker handling in conditional operations.

These tests verify that jokers interact correctly with ETag-based conditional
operations, particularly that etag verification still occurs even with jokers.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT, DELETE_CURRENT, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_keep_current_verifies_etag(tmpdir, DictToTest, kwargs):
    """Critical: KEEP_CURRENT with wrong etag should fail condition.

    This test verifies that even when KEEP_CURRENT is used (which doesn't modify
    the value), the etag is still checked. This is important for correctness.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    wrong_etag = "definitely_wrong_etag"

    result = d.set_item_if("key1", KEEP_CURRENT, wrong_etag, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert d["key1"] == "original"  # Value unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_keep_current_matching_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with matching etag succeeds and keeps value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    result = d.set_item_if("key1", KEEP_CURRENT, etag, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert d["key1"] == "original"
    assert d.etag("key1") == etag  # Etag unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_delete_current_succeeds(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with matching etag deletes the key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if("key1", DELETE_CURRENT, etag, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_delete_current_fails_on_wrong_etag(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with wrong etag fails condition and preserves key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.set_item_if("key1", DELETE_CURRENT, old_etag, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_with_keep_current_verifies_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with unchanged etag fails condition."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    current_etag = d.etag("key1")

    result = d.set_item_if("key1", KEEP_CURRENT, current_etag, ETAG_HAS_CHANGED)

    assert not result.condition_was_satisfied
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_with_keep_current_changed_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with changed etag succeeds (no modification)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"

    result = d.set_item_if("key1", KEEP_CURRENT, old_etag, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert d["key1"] == "modified"  # Value stays as modified


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_with_delete_current_succeeds(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with changed etag deletes the key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"

    result = d.set_item_if("key1", DELETE_CURRENT, old_etag, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_with_delete_current_unchanged_etag(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with unchanged etag fails condition."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.set_item_if("key1", DELETE_CURRENT, current_etag, ETAG_HAS_CHANGED)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_joker_keep_current_on_missing_key_conditional(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT on missing key with conditional set does not raise.

    New API treats missing keys as actual_etag=ITEM_NOT_AVAILABLE.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.set_item_if("nonexistent", KEEP_CURRENT, "some_etag", ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_joker_delete_current_on_missing_key_conditional(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT on missing key with conditional set does not raise.

    New API treats missing keys as actual_etag=ITEM_NOT_AVAILABLE.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.set_item_if("nonexistent", DELETE_CURRENT, "some_etag", ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_preserves_exact_value(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT doesn't alter value in any way."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    original = {"complex": [1, 2, 3], "nested": {"a": "b"}}
    d["key1"] = original
    etag = d.etag("key1")

    d.set_item_if("key1", KEEP_CURRENT, etag, ETAG_IS_THE_SAME)

    assert d["key1"] == original


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_removes_key_completely(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT removes key from iteration and containment checks."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    d["key2"] = "value2"
    etag = d.etag("key1")

    d.set_item_if("key1", DELETE_CURRENT, etag, ETAG_IS_THE_SAME)

    assert "key1" not in d
    assert "key1" not in list(d.keys())
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_jokers_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify jokers work correctly with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    # Test KEEP_CURRENT
    result = d.set_item_if(key, KEEP_CURRENT, etag, ETAG_IS_THE_SAME)
    assert result.condition_was_satisfied
    assert d[key] == "value"

    # Test DELETE_CURRENT
    result = d.set_item_if(key, DELETE_CURRENT, etag, ETAG_IS_THE_SAME)
    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_does_not_update_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT doesn't change the etag (value not touched)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag_before = d.etag("key1")

    d.set_item_if("key1", KEEP_CURRENT, etag_before, ETAG_IS_THE_SAME)
    etag_after = d.etag("key1")

    assert etag_before == etag_after


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_with_unknown_etag_fails(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with ITEM_NOT_AVAILABLE fails condition."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if("key1", KEEP_CURRENT, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_with_unknown_etag_fails(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with ITEM_NOT_AVAILABLE fails condition."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if("key1", DELETE_CURRENT, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert "key1" in d
