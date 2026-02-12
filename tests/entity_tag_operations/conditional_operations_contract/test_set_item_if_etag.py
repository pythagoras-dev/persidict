"""Tests for set_item_if_etag method."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_succeeds_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag stores value when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    result = d.set_item_if("key1", "updated", ETAG_IS_THE_SAME, etag)

    assert result.condition_was_satisfied
    assert result.resulting_etag != etag  # New etag returned
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_fails_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag returns ETAG_HAS_CHANGED when etag mismatches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes for all backends
    d["key1"] = "modified"
    current_etag = d.etag("key1")

    result = d.set_item_if("key1", "should_not_set", ETAG_IS_THE_SAME, old_etag)

    assert not result.condition_was_satisfied
    assert d["key1"] == "modified"  # Original value preserved
    assert d.etag("key1") == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify set_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.set_item_if("nonexistent", "value", ETAG_IS_THE_SAME, "some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_unknown_etag_returns_changed(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag with ITEM_NOT_AVAILABLE returns ETAG_HAS_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"

    result = d.set_item_if("key1", "new_value", ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert d["key1"] == "original"  # Value unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_unknown_etag_missing_key_raises(tmpdir, DictToTest, kwargs):
    """Verify set_item_if with ITEM_NOT_AVAILABLE on missing key evaluates condition."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.set_item_if("nonexistent", "value", ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE)
    # ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE => condition satisfied, value is set
    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_succeeds_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag stores value when etag has changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.set_item_if("key1", "updated_again", ETAG_HAS_CHANGED, old_etag)

    assert result.condition_was_satisfied
    assert d["key1"] == "updated_again"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_fails_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    current_etag = d.etag("key1")

    result = d.set_item_if("key1", "should_not_set", ETAG_HAS_CHANGED, current_etag)

    assert not result.condition_was_satisfied
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify set_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.set_item_if("nonexistent", "value", ETAG_HAS_CHANGED, "some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    etag = d.etag(key)

    result = d.set_item_if(key, "updated", ETAG_IS_THE_SAME, etag)

    assert result.condition_was_satisfied
    assert d[key] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_different_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.set_item_if(key, "updated_again", ETAG_HAS_CHANGED, old_etag)

    assert result.condition_was_satisfied
    assert d[key] == "updated_again"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_returns_new_etag_different_from_old(tmpdir, DictToTest, kwargs):
    """Verify the returned etag differs from the one passed in after successful update."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    result = d.set_item_if("key1", "updated", ETAG_IS_THE_SAME, old_etag)

    assert result.condition_was_satisfied
    new_etag = result.resulting_etag
    assert new_etag != old_etag
    assert d.etag("key1") == new_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_equal_with_same_value(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag works even when setting the same value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if("key1", "value", ETAG_IS_THE_SAME, etag)

    # Should still succeed (value is written regardless)
    assert result.condition_was_satisfied
    assert d["key1"] == "value"
