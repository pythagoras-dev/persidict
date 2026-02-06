"""Tests for delete_item_if_etag_not_changed and delete_item_if_etag_changed methods."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_HAS_NOT_CHANGED, ETAG_HAS_CHANGED, ETAG_UNKNOWN
)

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_succeeds_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed deletes key when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.delete_item_if_etag_not_changed("key1", etag)

    assert result is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_fails_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed returns ETAG_HAS_CHANGED when etag mismatches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.delete_item_if_etag_not_changed("key1", old_etag)

    assert result is ETAG_HAS_CHANGED
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.delete_item_if_etag_not_changed("nonexistent", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_with_unknown_etag_returns_changed(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed with ETAG_UNKNOWN returns ETAG_HAS_CHANGED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.delete_item_if_etag_not_changed("key1", ETAG_UNKNOWN)

    assert result is ETAG_HAS_CHANGED
    assert "key1" in d  # Key not deleted


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_with_unknown_etag_missing_key_raises(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed with ETAG_UNKNOWN raises KeyError for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.delete_item_if_etag_not_changed("nonexistent", ETAG_UNKNOWN)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_changed_succeeds_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_changed deletes key when etag has changed."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.delete_item_if_etag_changed("key1", old_etag)

    assert result is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_changed_fails_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_changed returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.delete_item_if_etag_changed("key1", current_etag)

    assert result is ETAG_HAS_NOT_CHANGED
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_changed_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_changed raises error for missing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises((KeyError, FileNotFoundError)):
        d.delete_item_if_etag_changed("nonexistent", "some_etag")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_not_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    result = d.delete_item_if_etag_not_changed(key, etag)

    assert result is None
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.delete_item_if_etag_changed(key, old_etag)

    assert result is None
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_not_changed_verifies_etag_before_delete(tmpdir, DictToTest, kwargs):
    """Verify delete checks etag before performing deletion."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    correct_etag = d.etag("key1")
    wrong_etag = "definitely_wrong_etag_value"

    result = d.delete_item_if_etag_not_changed("key1", wrong_etag)

    assert result is ETAG_HAS_CHANGED
    assert "key1" in d
    # Now try with correct etag
    result2 = d.delete_item_if_etag_not_changed("key1", correct_etag)
    assert result2 is None
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_changed_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag_changed behavior with ETAG_UNKNOWN."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # ETAG_UNKNOWN differs from actual etag (S3 always has etags)
    result = d.delete_item_if_etag_changed("key1", ETAG_UNKNOWN)

    # Should succeed since ETAG_UNKNOWN != actual_etag
    assert result is None
    assert "key1" not in d
