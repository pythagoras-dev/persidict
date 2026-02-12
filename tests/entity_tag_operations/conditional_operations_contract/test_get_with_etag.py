"""Tests for get_with_etag convenience method."""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ANY_ETAG, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME,
    ConditionalOperationResult,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_returns_value_and_etag(tmpdir, DictToTest, kwargs):
    """Value and ETag are returned for an existing key."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "hello"
    expected_etag = d.etag("key1")

    result = d.get_with_etag("key1")

    assert isinstance(result, ConditionalOperationResult)
    assert result.new_value == "hello"
    assert result.actual_etag == expected_etag
    assert result.resulting_etag == expected_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_missing_key(tmpdir, DictToTest, kwargs):
    """Missing key yields ITEM_NOT_AVAILABLE in all relevant fields."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.get_with_etag("nonexistent")

    assert result.new_value is ITEM_NOT_AVAILABLE
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_condition_fields(tmpdir, DictToTest, kwargs):
    """Condition metadata reflects an unconditional read."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = 42

    result = d.get_with_etag("k")

    assert result.condition_was_satisfied is True
    assert result.requested_condition is ANY_ETAG


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_reflects_latest_value(tmpdir, DictToTest, kwargs):
    """After an update, get_with_etag returns the new value and a new ETag."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    r1 = d.get_with_etag("k")

    time.sleep(1.1)  # Ensure distinct timestamps on coarse-grained backends
    d["k"] = "v2"
    r2 = d.get_with_etag("k")

    assert r2.new_value == "v2"
    assert r2.actual_etag != r1.actual_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_etag_usable_for_cas(tmpdir, DictToTest, kwargs):
    """The ETag from get_with_etag can drive a successful set_item_if."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["counter"] = 10

    r = d.get_with_etag("counter")
    write = d.set_item_if("counter", value=r.new_value + 1, condition=ETAG_IS_THE_SAME, expected_etag=r.actual_etag)

    assert write.condition_was_satisfied
    assert d["counter"] == 11


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_tuple_key(tmpdir, DictToTest, kwargs):
    """Hierarchical tuple keys work correctly."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    key = ("section", "subsection", "leaf")
    d[key] = {"nested": True}

    result = d.get_with_etag(key)

    assert result.new_value == {"nested": True}
    assert isinstance(result.actual_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_complex_value(tmpdir, DictToTest, kwargs):
    """Complex values are correctly deserialized."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    value = {"list": [1, 2, 3], "nested": {"a": True, "b": None}}
    d["k"] = value

    result = d.get_with_etag("k")

    assert result.new_value == value
