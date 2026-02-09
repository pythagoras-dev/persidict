"""Tests for discard_item_if_etag method.

These methods have soft-delete semantics: they return bool instead of raising
exceptions for missing keys.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED

from tests.data_for_mutable_tests import mutable_tests

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is satisfied when key is deleted."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.discard_item_if("key1", etag, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_returns_false_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is not satisfied when etag mismatches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if("key1", old_etag, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_returns_false_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is not satisfied for missing keys (no exception)."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.discard_item_if("nonexistent", "some_etag", ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is not satisfied with ITEM_NOT_AVAILABLE for existing key."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.discard_item_if("key1", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert "key1" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_different_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is satisfied when key is deleted."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if("key1", old_etag, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_different_returns_false_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if condition is not satisfied when etag matches."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.discard_item_if("key1", current_etag, ETAG_HAS_CHANGED)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_different_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if on missing key: 'some_etag' != ITEM_NOT_AVAILABLE => satisfied."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.discard_item_if("nonexistent", "some_etag", ETAG_HAS_CHANGED)

    # "some_etag" != ITEM_NOT_AVAILABLE, so ETAG_HAS_CHANGED is satisfied
    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_different_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if ETAG_HAS_CHANGED behavior with ITEM_NOT_AVAILABLE."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"

    # ITEM_NOT_AVAILABLE differs from actual etag, so discard should succeed
    result = d.discard_item_if("key1", ITEM_NOT_AVAILABLE, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    result = d.discard_item_if(key, etag, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_different_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if_etag works with hierarchical tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.discard_item_if(key, old_etag, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_return_type_is_bool(tmpdir, DictToTest, kwargs):
    """Verify discard methods always return ConditionalOperationResult."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result_success = d.discard_item_if("key1", etag, ETAG_IS_THE_SAME)
    assert result_success.condition_was_satisfied

    d["key2"] = "value"
    old_etag = d.etag("key2")
    time.sleep(1.1)
    d["key2"] = "modified"

    result_changed = d.discard_item_if("key2", old_etag, ETAG_IS_THE_SAME)
    assert not result_changed.condition_was_satisfied

    result_missing = d.discard_item_if("nonexistent", "etag", ETAG_IS_THE_SAME)
    assert not result_missing.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_item_if_etag_equal_idempotent_for_missing(tmpdir, DictToTest, kwargs):
    """Verify discard on missing key with various expected etags."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result1 = d.discard_item_if("nonexistent", "etag1", ETAG_IS_THE_SAME)
    result2 = d.discard_item_if("nonexistent", "etag2", ETAG_IS_THE_SAME)
    result3 = d.discard_item_if("nonexistent", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    # "etag1" != ITEM_NOT_AVAILABLE -> not satisfied
    assert not result1.condition_was_satisfied
    # "etag2" != ITEM_NOT_AVAILABLE -> not satisfied
    assert not result2.condition_was_satisfied
    # ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE -> satisfied (both agree key absent)
    assert result3.condition_was_satisfied
