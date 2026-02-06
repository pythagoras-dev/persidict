"""Tests for get_item_if_etag method."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_HAS_NOT_CHANGED, ETAG_HAS_CHANGED, ETAG_UNKNOWN,
    EQUAL_ETAG, DIFFERENT_ETAG
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_value_when_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns (value, new_etag) when etag has changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if_etag("key1", old_etag, DIFFERENT_ETAG)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, new_etag = result
    assert value == "modified"
    assert isinstance(new_etag, str)
    assert new_etag != old_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_flag_when_unchanged(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if_etag("key1", current_etag, DIFFERENT_ETAG)

    assert result is ETAG_HAS_NOT_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.get_item_if_etag("nonexistent", "some_etag", DIFFERENT_ETAG)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_value_when_matches(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns (value, etag) when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if_etag("key1", current_etag, EQUAL_ETAG)

    assert result is not ETAG_HAS_CHANGED
    value, returned_etag = result
    assert value == "value"
    assert returned_etag == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_flag_when_differs(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns ETAG_HAS_CHANGED when etag differs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if_etag("key1", old_etag, EQUAL_ETAG)

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_equal_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.get_item_if_etag("nonexistent", "some_etag", EQUAL_ETAG)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_with_tuple_keys_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag works with hierarchical tuple keys when changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.get_item_if_etag(key, old_etag, DIFFERENT_ETAG)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, _ = result
    assert value == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_with_tuple_keys_not_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag works with hierarchical tuple keys when equal."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    current_etag = d.etag(key)

    result = d.get_item_if_etag(key, current_etag, EQUAL_ETAG)

    assert result is not ETAG_HAS_CHANGED
    value, _ = result
    assert value == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_correct_complex_values(tmpdir, DictToTest, kwargs):
    """Verify returned values are correctly deserialized for complex types."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    complex_value = {"nested": {"list": [1, 2, 3], "bool": True}}
    d["key1"] = complex_value
    current_etag = d.etag("key1")

    result = d.get_item_if_etag("key1", current_etag, EQUAL_ETAG)

    assert result is not ETAG_HAS_CHANGED
    value, _ = result
    assert value == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_different_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag DIFFERENT_ETAG behavior with ETAG_UNKNOWN."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # ETAG_UNKNOWN differs from actual etag, so should return value
    result = d.get_item_if_etag("key1", ETAG_UNKNOWN, DIFFERENT_ETAG)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, etag = result
    assert value == "value"
    assert isinstance(etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_equal_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag EQUAL_ETAG behavior with ETAG_UNKNOWN."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # ETAG_UNKNOWN differs from actual etag, so should return ETAG_HAS_CHANGED
    result = d.get_item_if_etag("key1", ETAG_UNKNOWN, EQUAL_ETAG)

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returned_etag_matches_current(tmpdir, DictToTest, kwargs):
    """Verify the etag returned by get_item_if_etag matches current etag."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"
    expected_etag = d.etag("key1")

    result = d.get_item_if_etag("key1", old_etag, DIFFERENT_ETAG)

    assert result is not ETAG_HAS_NOT_CHANGED
    _, returned_etag = result
    assert returned_etag == expected_etag
