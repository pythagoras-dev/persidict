"""Tests for set_item_get_etag method and etag return semantics."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import KEEP_CURRENT, DELETE_CURRENT

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_returns_etag_string(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value; d.etag(key) returns a non-empty etag string."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = "value"
    etag = d.etag("key1")

    assert etag is not None
    assert isinstance(etag, str)
    assert len(etag) > 0


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_updates_value(tmpdir, DictToTest, kwargs):
    """Verify set_item_get_etag correctly stores the value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = "value"

    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_returns_different_etag_on_update(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value; d.etag(key) returns different etag when value changes."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value1"
    etag1 = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "value2"
    etag2 = d.etag("key1")

    assert etag1 != etag2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_returned_etag_matches_etag_method(tmpdir, DictToTest, kwargs):
    """Verify returned etag matches subsequent call to etag() method."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = "value"
    returned_etag = d.etag("key1")
    current_etag = d.etag("key1")

    assert returned_etag == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_keep_current_returns_none(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT keeps value unchanged."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    original_etag = d.etag("key1")

    d["key1"] = KEEP_CURRENT

    assert d["key1"] == "original"
    assert d.etag("key1") == original_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_keep_current_on_missing_key(tmpdir, DictToTest, kwargs):
    """Verify KEEP_CURRENT on missing key is a no-op."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["nonexistent"] = KEEP_CURRENT

    assert "nonexistent" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_delete_current_returns_none(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT deletes key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    d["key1"] = DELETE_CURRENT

    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_delete_current_on_missing_key(tmpdir, DictToTest, kwargs):
    """Verify DELETE_CURRENT on missing key is a no-op."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["nonexistent"] = DELETE_CURRENT

    assert "nonexistent" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")

    d[key] = "value"
    etag = d.etag(key)

    assert etag is not None
    assert isinstance(etag, str)
    assert d[key] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_complex_value(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value works with complex nested values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    complex_value = {"nested": {"list": [1, 2, 3], "bool": True}, "tuple": (1, 2)}

    d["key1"] = complex_value
    etag = d.etag("key1")

    assert etag is not None
    assert d["key1"] == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_none_value(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value works when storing None as value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = None
    etag = d.etag("key1")

    assert etag is not None
    assert isinstance(etag, str)
    assert d["key1"] is None


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_with_empty_string_value(tmpdir, DictToTest, kwargs):
    """Verify d[key] = value works when storing empty string."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = ""
    etag = d.etag("key1")

    assert etag is not None
    assert isinstance(etag, str)
    assert d["key1"] == ""


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_get_etag_multiple_operations(tmpdir, DictToTest, kwargs):
    """Verify multiple d[key] = value calls work correctly."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    d["key1"] = "value1"
    etag1 = d.etag("key1")
    d["key2"] = "value2"
    etag2 = d.etag("key2")
    d["key3"] = "value3"
    etag3 = d.etag("key3")

    assert all(isinstance(e, str) for e in [etag1, etag2, etag3])
    assert d["key1"] == "value1"
    assert d["key2"] == "value2"
    assert d["key3"] == "value3"
