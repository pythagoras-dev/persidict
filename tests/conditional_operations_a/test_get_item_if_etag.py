"""Tests for get_item_if_etag_changed and get_item_if_etag_not_changed methods."""

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
def test_get_item_if_etag_changed_returns_value_when_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_changed returns (value, new_etag) when etag has changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if_etag_changed("key1", old_etag)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, new_etag = result
    assert value == "modified"
    assert isinstance(new_etag, str)
    assert new_etag != old_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_changed_returns_flag_when_unchanged(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_changed returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if_etag_changed("key1", current_etag)

    assert result is ETAG_HAS_NOT_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.get_item_if_etag_changed("nonexistent", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_not_changed_returns_value_when_matches(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_not_changed returns (value, etag) when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if_etag_not_changed("key1", current_etag)

    assert result is not ETAG_HAS_CHANGED
    value, returned_etag = result
    assert value == "value"
    assert returned_etag == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_not_changed_returns_flag_when_differs(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_not_changed returns ETAG_HAS_CHANGED when etag differs."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if_etag_not_changed("key1", old_etag)

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_not_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_not_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.get_item_if_etag_not_changed("nonexistent", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.get_item_if_etag_changed(key, old_etag)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, _ = result
    assert value == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_not_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_not_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    current_etag = d.etag(key)

    result = d.get_item_if_etag_not_changed(key, current_etag)

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

    result = d.get_item_if_etag_not_changed("key1", current_etag)

    assert result is not ETAG_HAS_CHANGED
    value, _ = result
    assert value == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_changed_with_none_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_changed behavior with None etag."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # None etag differs from actual etag, so should return value
    result = d.get_item_if_etag_changed("key1", None)

    assert result is not ETAG_HAS_NOT_CHANGED
    value, etag = result
    assert value == "value"
    assert isinstance(etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_not_changed_with_none_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag_not_changed behavior with None etag."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # None etag differs from actual etag, so should return ETAG_HAS_CHANGED
    result = d.get_item_if_etag_not_changed("key1", None)

    assert result is ETAG_HAS_CHANGED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_changed_returned_etag_matches_current(tmpdir, DictToTest, kwargs):
    """Verify the etag returned by get_item_if_etag_changed matches current etag."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"
    expected_etag = d.etag("key1")

    result = d.get_item_if_etag_changed("key1", old_etag)

    assert result is not ETAG_HAS_NOT_CHANGED
    _, returned_etag = result
    assert returned_etag == expected_etag
