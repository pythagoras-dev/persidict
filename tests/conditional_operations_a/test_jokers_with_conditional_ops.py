"""Tests for KEEP_CURRENT and DELETE_CURRENT joker handling in conditional operations.

These tests verify that jokers interact correctly with ETag-based conditional
operations, particularly that etag verification still occurs even with jokers.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_HAS_NOT_CHANGED, ETAG_HAS_CHANGED, ETAG_UNKNOWN,
    KEEP_CURRENT, DELETE_CURRENT
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_keep_current_verifies_etag(tmpdir, DictToTest, kwargs):
    """Critical: KEEP_CURRENT with wrong etag should return ETAG_HAS_CHANGED.

    This test verifies that even when KEEP_CURRENT is used (which doesn't modify
    the value), the etag is still checked. This is important for correctness.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    correct_etag = d.etag("key1")
    wrong_etag = "definitely_wrong_etag"

    result = d.set_item_if_etag_not_changed("key1", KEEP_CURRENT, wrong_etag)

    assert result is ETAG_HAS_CHANGED
    assert d["key1"] == "original"  # Value unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_keep_current_matching_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with matching etag returns None and keeps value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", KEEP_CURRENT, etag)

    assert result is None  # KEEP_CURRENT always returns None on success
    assert d["key1"] == "original"
    assert d.etag("key1") == etag  # Etag unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_delete_current_succeeds(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with matching etag deletes the key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", DELETE_CURRENT, etag)

    assert result is None  # DELETE_CURRENT returns None on success
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_delete_current_fails_on_wrong_etag(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with wrong etag returns ETAG_HAS_CHANGED and preserves key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.set_item_if_etag_not_changed("key1", DELETE_CURRENT, old_etag)

    assert result is ETAG_HAS_CHANGED
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_with_keep_current_verifies_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with unchanged etag returns ETAG_HAS_NOT_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    current_etag = d.etag("key1")

    result = d.set_item_if_etag_changed("key1", KEEP_CURRENT, current_etag)

    assert result is ETAG_HAS_NOT_CHANGED
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_with_keep_current_changed_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with changed etag returns None (success, no modification)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"

    result = d.set_item_if_etag_changed("key1", KEEP_CURRENT, old_etag)

    assert result is None  # KEEP_CURRENT returns None
    assert d["key1"] == "modified"  # Value stays as modified


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_with_delete_current_succeeds(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with changed etag deletes the key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"

    result = d.set_item_if_etag_changed("key1", DELETE_CURRENT, old_etag)

    assert result is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_with_delete_current_unchanged_etag(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with unchanged etag returns ETAG_HAS_NOT_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.set_item_if_etag_changed("key1", DELETE_CURRENT, current_etag)

    assert result is ETAG_HAS_NOT_CHANGED
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_joker_keep_current_on_missing_key_conditional(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT on missing key with conditional set raises KeyError."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.set_item_if_etag_not_changed("nonexistent", KEEP_CURRENT, "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_joker_delete_current_on_missing_key_conditional(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT on missing key with conditional set raises KeyError."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.set_item_if_etag_not_changed("nonexistent", DELETE_CURRENT, "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_preserves_exact_value(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT doesn't alter value in any way."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    original = {"complex": [1, 2, 3], "nested": {"a": "b"}}
    d["key1"] = original
    etag = d.etag("key1")

    d.set_item_if_etag_not_changed("key1", KEEP_CURRENT, etag)

    assert d["key1"] == original


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_removes_key_completely(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT removes key from iteration and containment checks."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    d["key2"] = "value2"
    etag = d.etag("key1")

    d.set_item_if_etag_not_changed("key1", DELETE_CURRENT, etag)

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
    result = d.set_item_if_etag_not_changed(key, KEEP_CURRENT, etag)
    assert result is None
    assert d[key] == "value"

    # Test DELETE_CURRENT
    result = d.set_item_if_etag_not_changed(key, DELETE_CURRENT, etag)
    assert result is None
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_does_not_update_etag(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT doesn't change the etag (value not touched)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag_before = d.etag("key1")

    d.set_item_if_etag_not_changed("key1", KEEP_CURRENT, etag_before)
    etag_after = d.etag("key1")

    assert etag_before == etag_after


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_with_unknown_etag_fails(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT with ETAG_UNKNOWN returns ETAG_HAS_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if_etag_not_changed("key1", KEEP_CURRENT, ETAG_UNKNOWN)

    assert result is ETAG_HAS_CHANGED
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_with_unknown_etag_fails(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT with ETAG_UNKNOWN returns ETAG_HAS_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.set_item_if_etag_not_changed("key1", DELETE_CURRENT, ETAG_UNKNOWN)

    assert result is ETAG_HAS_CHANGED
    assert "key1" in d
