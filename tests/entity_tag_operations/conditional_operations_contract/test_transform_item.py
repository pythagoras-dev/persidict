"""Tests for transform_item with joker values and basic operations.

These tests verify that transform_item correctly handles KEEP_CURRENT,
DELETE_CURRENT, and regular values across all dict backends.
"""

import pytest
from moto import mock_aws

import persidict.persi_dict as persi_dict
from persidict import LocalDict, ConcurrencyConflictError
from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT, DELETE_CURRENT,
    IF_ETAG_CHANGED,
    ConditionalOperationResult,
    VALUE_NOT_RETRIEVED,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_returns_new_value(tmpdir, DictToTest, kwargs):
    """Transformer returning a new value updates the stored value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"

    result = d.transform_item("key1", transformer=lambda v: v + "_transformed")

    assert result.new_value == "original_transformed"
    assert d["key1"] == "original_transformed"
    assert isinstance(result.resulting_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_returns_delete_current(tmpdir, DictToTest, kwargs):
    """Transformer returning DELETE_CURRENT removes the key."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"

    result = d.transform_item("key1", transformer=lambda v: DELETE_CURRENT)

    assert result.new_value is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_returns_delete_current_missing_key(tmpdir, DictToTest, kwargs):
    """DELETE_CURRENT on missing key is a no-op, no error."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.transform_item("nonexistent", transformer=lambda v: DELETE_CURRENT)

    assert result.new_value is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "nonexistent" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_returns_keep_current(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT leaves value unchanged and returns actual value, not sentinel."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "original"
    etag_before = d.etag("key1")

    result = d.transform_item("key1", transformer=lambda v: KEEP_CURRENT)

    assert result.new_value == "original"
    assert result.new_value is not KEEP_CURRENT
    assert result.resulting_etag == etag_before
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_returns_keep_current_missing_key(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT on missing key returns ITEM_NOT_AVAILABLE, key stays absent."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.transform_item("nonexistent", transformer=lambda v: KEEP_CURRENT)

    assert result.new_value is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "nonexistent" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_receives_current_value(tmpdir, DictToTest, kwargs):
    """Transformer receives the actual stored value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = {"nested": [1, 2, 3]}
    received = []

    d.transform_item("key1", transformer=lambda v: (received.append(v), v)[1])

    assert len(received) == 1
    assert received[0] == {"nested": [1, 2, 3]}


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_receives_item_not_available_for_missing_key(tmpdir, DictToTest, kwargs):
    """Transformer receives ITEM_NOT_AVAILABLE when key is absent."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    received = []

    d.transform_item("nonexistent", transformer=lambda v: (received.append(v), KEEP_CURRENT)[1])

    assert len(received) == 1
    assert received[0] is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_creates_new_key(tmpdir, DictToTest, kwargs):
    """Transformer can create a new key from ITEM_NOT_AVAILABLE input."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.transform_item("new_key", transformer=lambda v: "created")

    assert result.new_value == "created"
    assert isinstance(result.resulting_etag, str)
    assert d["new_key"] == "created"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_keep_current_does_not_change_etag(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT preserves the exact etag (no write occurs)."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key1"] = "value"
    etag_before = d.etag("key1")

    result = d.transform_item("key1", transformer=lambda v: KEEP_CURRENT)
    etag_after = d.etag("key1")

    assert etag_before == etag_after
    assert result.resulting_etag == etag_before


def test_transform_conflict_retries_then_raises(monkeypatch):
    """Conflicts past n_retries raise ConcurrencyConflictError."""
    d = LocalDict(serialization_format="pkl")
    d["key"] = "value"

    def always_conflict_set_item_if(key, value, condition, expected_etag, *,
                                    retrieve_value=IF_ETAG_CHANGED):
        return ConditionalOperationResult(
            condition_was_satisfied=False,
            requested_condition=condition,
            actual_etag=expected_etag,
            resulting_etag=expected_etag,
            new_value=VALUE_NOT_RETRIEVED,
        )

    monkeypatch.setattr(d, "set_item_if", always_conflict_set_item_if)
    monkeypatch.setattr(persi_dict.time, "sleep", lambda _: None)
    calls = []

    def transformer(value):
        calls.append(value)
        return "new"

    with pytest.raises(ConcurrencyConflictError) as excinfo:
        d.transform_item("key", transformer=transformer, n_retries=1)

    assert excinfo.value.attempts == 2
    assert len(calls) == 2


@pytest.mark.parametrize(
    "n_retries, exc_type",
    [(-1, ValueError), ("bad", TypeError)],
)
def test_transform_rejects_invalid_n_retries(n_retries, exc_type):
    """Invalid n_retries values raise a clear error."""
    d = LocalDict(serialization_format="pkl")
    d["key"] = "value"

    with pytest.raises(exc_type):
        d.transform_item("key", transformer=lambda v: v, n_retries=n_retries)
