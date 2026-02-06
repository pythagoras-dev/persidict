"""Tests for discard_item_if_etag_not_changed and discard_item_if_etag_changed methods.

These methods have soft-delete semantics: they return bool instead of raising
exceptions for missing keys.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import ETAG_UNKNOWN

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_not_changed returns True when key is deleted."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.discard_item_if_etag_not_changed("key1", etag)

    assert result is True
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_returns_false_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_not_changed returns False when etag mismatches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if_etag_not_changed("key1", old_etag)

    assert result is False
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_returns_false_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_not_changed returns False for missing keys (no exception)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.discard_item_if_etag_not_changed("nonexistent", "some_etag")

    assert result is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_not_changed returns False with ETAG_UNKNOWN for existing key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.discard_item_if_etag_not_changed("key1", ETAG_UNKNOWN)

    assert result is False
    assert "key1" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_changed_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_changed returns True when key is deleted."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if_etag_changed("key1", old_etag)

    assert result is True
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_changed_returns_false_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_changed returns False when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.discard_item_if_etag_changed("key1", current_etag)

    assert result is False
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_changed_returns_false_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_changed returns False for missing keys (no exception)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.discard_item_if_etag_changed("nonexistent", "some_etag")

    assert result is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_changed_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_changed behavior with ETAG_UNKNOWN."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # ETAG_UNKNOWN differs from actual etag, so discard should succeed
    result = d.discard_item_if_etag_changed("key1", ETAG_UNKNOWN)

    assert result is True
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_not_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    result = d.discard_item_if_etag_not_changed(key, etag)

    assert result is True
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_changed_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag_changed works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.discard_item_if_etag_changed(key, old_etag)

    assert result is True
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_return_type_is_bool(tmpdir, DictToTest, kwargs):
    """Verify discard methods always return bool, never status flags."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result_success = d.discard_item_if_etag_not_changed("key1", etag)
    assert isinstance(result_success, bool)

    d["key2"] = "value"
    old_etag = d.etag("key2")
    time.sleep(1.1)
    d["key2"] = "modified"

    result_changed = d.discard_item_if_etag_not_changed("key2", old_etag)
    assert isinstance(result_changed, bool)

    result_missing = d.discard_item_if_etag_not_changed("nonexistent", "etag")
    assert isinstance(result_missing, bool)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_not_changed_idempotent_for_missing(tmpdir, DictToTest, kwargs):
    """Verify calling discard on missing key multiple times returns False consistently."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result1 = d.discard_item_if_etag_not_changed("nonexistent", "etag1")
    result2 = d.discard_item_if_etag_not_changed("nonexistent", "etag2")
    result3 = d.discard_item_if_etag_not_changed("nonexistent", ETAG_UNKNOWN)

    assert result1 is False
    assert result2 is False
    assert result3 is False
