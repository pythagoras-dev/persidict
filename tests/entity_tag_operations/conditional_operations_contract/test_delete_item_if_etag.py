"""Tests for delete_item_if_etag method."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_succeeds_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag deletes key when etag matches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag = d.etag("key1")

    result = d.discard_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_fails_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag returns ETAG_HAS_CHANGED when etag mismatches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=old_etag)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_item_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_with_unknown_etag_returns_changed(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag with ITEM_NOT_AVAILABLE returns ETAG_HAS_CHANGED."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.discard_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert "key1" in d  # Key not deleted


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_with_unknown_etag_missing_key_raises(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if with ITEM_NOT_AVAILABLE on missing key evaluates condition."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_item_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)
    # ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE => condition satisfied, but key already absent
    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_different_succeeds_when_etag_differs(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag deletes key when etag has changed."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.discard_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_different_fails_when_etag_matches(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.discard_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=current_etag)

    assert not result.condition_was_satisfied
    assert "key1" in d
    assert d["key1"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_different_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify discard_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_item_if("nonexistent", condition=ETAG_HAS_CHANGED, expected_etag="some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag works with hierarchical tuple keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    etag = d.etag(key)

    result = d.discard_item_if(key, condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_different_with_tuple_keys(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag works with hierarchical tuple keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.discard_item_if(key, condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    assert key not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_equal_verifies_etag_before_delete(tmpdir, DictToTest, kwargs):
    """Verify delete checks etag before performing deletion."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    correct_etag = d.etag("key1")
    wrong_etag = "definitely_wrong_etag_value"

    result = d.discard_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=wrong_etag)

    assert not result.condition_was_satisfied
    assert "key1" in d
    # Now try with correct etag
    result2 = d.discard_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=correct_etag)
    assert result2.condition_was_satisfied
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_item_if_etag_different_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify delete_item_if_etag ETAG_HAS_CHANGED behavior with ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    # ITEM_NOT_AVAILABLE differs from actual etag (S3 always has etags)
    result = d.discard_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    # Should succeed since ITEM_NOT_AVAILABLE != actual_etag
    assert result.condition_was_satisfied
    assert "key1" not in d
