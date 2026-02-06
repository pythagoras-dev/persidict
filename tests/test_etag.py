"""Comprehensive tests for ETag-related methods across all backends."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_HAS_NOT_CHANGED, KEEP_CURRENT, DELETE_CURRENT, DIFFERENT_ETAG
)

from .data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_returns_string(tmpdir, DictToTest, kwargs):
    """Verify etag() returns a string for existing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"

    etag = d.etag("key1")

    assert isinstance(etag, str)
    assert len(etag) > 0


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_changes_on_update(tmpdir, DictToTest, kwargs):
    """Verify etag changes when value is updated."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    etag_before = d.etag("key1")

    # Use 1.1s sleep for backends with 1-second timestamp resolution (mocked S3)
    time.sleep(1.1)
    d["key1"] = "value2"
    etag_after = d.etag("key1")

    assert etag_before != etag_after


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_stable_without_update(tmpdir, DictToTest, kwargs):
    """Verify etag remains stable when value is not modified."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"

    etag1 = d.etag("key1")
    etag2 = d.etag("key1")

    assert etag1 == etag2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_missing_key_raises_error(tmpdir, DictToTest, kwargs):
    """Verify etag() raises an error for nonexistent keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    # Different backends may raise different errors (KeyError, FileNotFoundError)
    with pytest.raises((KeyError, FileNotFoundError)):
        d.etag("nonexistent")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_value_when_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns (value, new_etag) when etag differs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    old_etag = d.etag("key1")

    time.sleep(MIN_SLEEP)
    d["key1"] = "value2"

    result = d.get_item_if_etag("key1", old_etag, DIFFERENT_ETAG)

    assert result != ETAG_HAS_NOT_CHANGED
    value, new_etag = result
    assert value == "value2"
    assert new_etag != old_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_flag_when_unchanged(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    current_etag = d.etag("key1")

    result = d.get_item_if_etag("key1", current_etag, DIFFERENT_ETAG)

    assert result is ETAG_HAS_NOT_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_missing_key_raises_error(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag raises an error for nonexistent keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    # Different backends may raise different errors (KeyError, FileNotFoundError)
    with pytest.raises((KeyError, FileNotFoundError)):
        d.get_item_if_etag("nonexistent", "some_etag", DIFFERENT_ETAG)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_returns_new_etag(tmpdir, DictToTest, kwargs):
    """Verify set_item_get_etag stores value and returns an etag string."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    etag = d.set_item_get_etag("key1", "value1")

    assert etag is not None
    assert isinstance(etag, str)
    assert d["key1"] == "value1"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_keep_current(tmpdir, DictToTest, kwargs):
    """Verify set_item_get_etag with KEEP_CURRENT returns None and keeps value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    original_etag = d.etag("key1")

    result = d.set_item_get_etag("key1", KEEP_CURRENT)

    assert result is None
    assert d["key1"] == "original"
    assert d.etag("key1") == original_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_delete_current(tmpdir, DictToTest, kwargs):
    """Verify set_item_get_etag with DELETE_CURRENT returns None and deletes key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"

    result = d.set_item_get_etag("key1", DELETE_CURRENT)

    assert result is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_with_complex_keys(tmpdir, DictToTest, kwargs):
    """Verify etag works with tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"

    etag = d.etag(key)

    assert isinstance(etag, str)
    assert len(etag) > 0
