"""Tests for discard_if_etag method.

These methods have soft-delete semantics: they return bool instead of raising
exceptions for missing keys.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED

from tests.data_for_mutable_tests import mutable_tests, make_test_dict

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is satisfied when key is deleted."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_returns_false_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is not satisfied when etag mismatches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=old_etag)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_returns_false_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is not satisfied for missing keys (no exception)."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="some_etag")

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is not satisfied with ITEM_NOT_AVAILABLE for existing key."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert "key1" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_different_returns_true_when_deleted(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is satisfied when key is deleted."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_different_returns_false_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify discard_if condition is not satisfied when etag matches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.discard_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=current_etag)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_different_for_missing_key(tmpdir, DictToTest, kwargs):
    """Verify discard_if on missing key: 'some_etag' != ITEM_NOT_AVAILABLE => satisfied."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_if("nonexistent", condition=ETAG_HAS_CHANGED, expected_etag="some_etag")

    # "some_etag" != ITEM_NOT_AVAILABLE, so ETAG_HAS_CHANGED is satisfied
    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_different_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify discard_if ETAG_HAS_CHANGED behavior with ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    # ITEM_NOT_AVAILABLE differs from actual etag, so discard should succeed
    result = d.discard_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_if_etag works with hierarchical tuple keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    result = d.discard_if(key, condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_different_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify discard_if_etag works with hierarchical tuple keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.discard_if(key, condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_return_type_is_bool(tmpdir, DictToTest, kwargs):
    """Verify discard methods always return ConditionalOperationResult."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result_success = d.discard_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    assert result_success.condition_was_satisfied

    d["key2"] = "value"
    old_etag = d.etag("key2")
    time.sleep(1.1)
    d["key2"] = "modified"

    result_changed = d.discard_if("key2", condition=ETAG_IS_THE_SAME, expected_etag=old_etag)
    assert not result_changed.condition_was_satisfied

    result_missing = d.discard_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="etag")
    assert not result_missing.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_equal_idempotent_for_missing(tmpdir, DictToTest, kwargs):
    """Verify discard on missing key with various expected etags."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result1 = d.discard_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="etag1")
    result2 = d.discard_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="etag2")
    result3 = d.discard_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    # "etag1" != ITEM_NOT_AVAILABLE -> not satisfied
    assert not result1.condition_was_satisfied
    # "etag2" != ITEM_NOT_AVAILABLE -> not satisfied
    assert not result2.condition_was_satisfied
    # ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE -> satisfied (both agree key absent)
    assert result3.condition_was_satisfied
