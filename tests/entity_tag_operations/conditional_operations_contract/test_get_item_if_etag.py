"""Tests for get_item_if_etag method."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED,
    ALWAYS_RETRIEVE,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict

MIN_SLEEP = 0.02


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_value_when_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns (value, new_etag) when etag has changed."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    value = result.new_value
    new_etag = result.resulting_etag
    assert value == "modified"
    assert isinstance(new_etag, str)
    assert new_etag != old_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_flag_when_unchanged(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns ETAG_HAS_NOT_CHANGED when etag matches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=current_etag)

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.get_item_if("nonexistent", condition=ETAG_HAS_CHANGED, expected_etag="some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_value_when_matches(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns (value, etag) when etag matches."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    current_etag = d.etag("key1")

    result = d.get_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=current_etag,
                           retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    value = result.new_value
    returned_etag = result.resulting_etag
    assert value == "value"
    assert returned_etag == current_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_flag_when_differs(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag returns ETAG_HAS_CHANGED when etag differs."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)  # Ensure timestamp changes
    d["key1"] = "modified"

    result = d.get_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=old_etag)

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_equal_missing_key_raises_keyerror(tmpdir, DictToTest, kwargs):
    """Verify get_item_if returns ITEM_NOT_AVAILABLE for missing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.get_item_if("nonexistent", condition=ETAG_IS_THE_SAME, expected_etag="some_etag")
    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_with_tuple_keys_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag works with hierarchical tuple keys when changed."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "original"
    old_etag = d.etag(key)

    time.sleep(1.1)
    d[key] = "modified"

    result = d.get_item_if(key, condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    value = result.new_value
    assert value == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_with_tuple_keys_not_changed(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag works with hierarchical tuple keys when equal."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("prefix", "subkey", "leaf")
    d[key] = "value"
    current_etag = d.etag(key)

    result = d.get_item_if(key, condition=ETAG_IS_THE_SAME, expected_etag=current_etag,
                           retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    value = result.new_value
    assert value == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returns_correct_complex_values(tmpdir, DictToTest, kwargs):
    """Verify returned values are correctly deserialized for complex types."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    complex_value = {"nested": {"list": [1, 2, 3], "bool": True}}
    d["key1"] = complex_value
    current_etag = d.etag("key1")

    result = d.get_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=current_etag,
                           retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    value = result.new_value
    assert value == complex_value


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_different_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag ETAG_HAS_CHANGED behavior with ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    # ITEM_NOT_AVAILABLE differs from actual etag, so should return value
    result = d.get_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    value = result.new_value
    etag = result.resulting_etag
    assert value == "value"
    assert isinstance(etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_equal_with_unknown_etag(tmpdir, DictToTest, kwargs):
    """Verify get_item_if_etag ETAG_IS_THE_SAME behavior with ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    # ITEM_NOT_AVAILABLE differs from actual etag, so should return ETAG_HAS_CHANGED
    result = d.get_item_if("key1", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_etag_returned_etag_matches_current(tmpdir, DictToTest, kwargs):
    """Verify the etag returned by get_item_if_etag matches current etag."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    old_etag = d.etag("key1")

    time.sleep(1.1)
    d["key1"] = "modified"
    expected_etag = d.etag("key1")

    result = d.get_item_if("key1", condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    returned_etag = result.resulting_etag
    assert returned_etag == expected_etag
