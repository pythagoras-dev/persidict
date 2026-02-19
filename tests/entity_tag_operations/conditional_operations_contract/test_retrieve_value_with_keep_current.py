"""Tests for retrieve_value interaction with KEEP_CURRENT in set_item_if.

Verifies that when KEEP_CURRENT is used as the value and the condition is
satisfied, the retrieve_value parameter controls whether the existing value
is fetched and returned in the result.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ALWAYS_RETRIEVE,
    NEVER_RETRIEVE,
    IF_ETAG_CHANGED,
    ITEM_NOT_AVAILABLE,
    VALUE_NOT_RETRIEVED,
    KEEP_CURRENT,
    ANY_ETAG,
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


# ── ALWAYS_RETRIEVE ──────────────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_always_retrieve_returns_value(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ALWAYS_RETRIEVE: condition satisfied → existing value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.new_value == "hello"
    assert not result.value_was_mutated


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_always_retrieve_missing_key(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ALWAYS_RETRIEVE on absent key → ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "missing", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


# ── NEVER_RETRIEVE ───────────────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_never_retrieve_returns_not_retrieved(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + NEVER_RETRIEVE: condition satisfied → VALUE_NOT_RETRIEVED."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=NEVER_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "hello"


# ── IF_ETAG_CHANGED (default) ───────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_if_etag_changed_skips_when_same(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + IF_ETAG_CHANGED: etags match → VALUE_NOT_RETRIEVED."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_if_etag_changed_fetches_when_different(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + IF_ETAG_CHANGED: etags differ → fetches value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    old_etag = d.etag("k")

    time.sleep(1.1)
    d["k"] = "modified"

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag=old_etag,
        retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value == "modified"
    assert d["k"] == "modified"
