"""Tests for set_item_if_etag_not_changed and set_item_if_etag_changed methods."""

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
def test_set_item_if_etag_not_changed_succeeds_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed stores value when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", "updated", etag)

    assert isinstance(result, str)
    assert result != etag  # New etag returned
    assert d["key1"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_fails_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed returns ETAG_HAS_CHANGED when etag mismatches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes for all backends
    d["key1"] = "modified"
    current_etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", "should_not_set", old_etag)

    assert result is ETAG_HAS_CHANGED
    assert d["key1"] == "modified"  # Original value preserved
    assert d.etag("key1") == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.set_item_if_etag_not_changed("nonexistent", "value", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_none_etag_returns_changed(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed with None etag returns ETAG_HAS_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"

    result = d.set_item_if_etag_not_changed("key1", "new_value", None)

    assert result is ETAG_HAS_CHANGED
    assert d["key1"] == "original"  # Value unchanged


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_none_etag_missing_key_raises(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed with None etag raises KeyError for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.set_item_if_etag_not_changed("nonexistent", "value", None)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_succeeds_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_changed stores value when etag has changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.set_item_if_etag_changed("key1", "updated_again", old_etag)

    assert isinstance(result, str)
    assert d["key1"] == "updated_again"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_fails_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_changed returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    current_etag = d.etag("key1")

    result = d.set_item_if_etag_changed("key1", "should_not_set", current_etag)

    assert result is ETAG_HAS_NOT_CHANGED
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.set_item_if_etag_changed("nonexistent", "value", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    etag = d.etag(key)

    result = d.set_item_if_etag_not_changed(key, "updated", etag)

    assert isinstance(result, str)
    assert d[key] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.set_item_if_etag_changed(key, "updated_again", old_etag)

    assert isinstance(result, str)
    assert d[key] == "updated_again"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_returns_new_etag_different_from_old(tmpdir, DictToTest, kwargs):
    """Verify the returned etag differs from the one passed in after successful update."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    new_etag = d.set_item_if_etag_not_changed("key1", "updated", old_etag)

    assert new_etag is not ETAG_HAS_CHANGED
    assert new_etag != old_etag
    assert d.etag("key1") == new_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_etag_not_changed_with_same_value(tmpdir, DictToTest, kwargs):
    """Verify set_item_if_etag_not_changed works even when setting the same value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.set_item_if_etag_not_changed("key1", "value", etag)

    # Should still succeed (value is written regardless)
    assert result is not ETAG_HAS_CHANGED
    assert d["key1"] == "value"
