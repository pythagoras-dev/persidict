"""Tests that the default retrieve_value is IF_ETAG_CHANGED.

The default was changed from ALWAYS_RETRIEVE to IF_ETAG_CHANGED so
that the IO-efficient path is the default for all conditional methods.
These tests pin down the new contract:

- When called without retrieve_value and expected_etag matches actual,
  new_value is VALUE_NOT_RETRIEVED (value not fetched).
- When called without retrieve_value and expected_etag differs from
  actual, new_value contains the real value (value fetched).
- get_with_etag always retrieves the value regardless of the default.
"""

import time
import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
    ITEM_NOT_AVAILABLE,
    VALUE_NOT_RETRIEVED,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


# ── get_item_if default ────────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_default_skips_value_when_etag_matches(
        tmpdir, DictToTest, kwargs):
    """Default retrieve_value skips fetch when expected_etag == actual_etag."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert result.resulting_etag == etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_default_fetches_value_when_etag_differs(
        tmpdir, DictToTest, kwargs):
    """Default retrieve_value fetches value when expected_etag != actual_etag."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    old_etag = d.etag("k")

    time.sleep(1.1)
    d["k"] = "modified"

    result = d.get_item_if(
        "k", condition=ETAG_HAS_CHANGED, expected_etag=old_etag)

    assert result.condition_was_satisfied
    assert result.new_value == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_default_absent_key(tmpdir, DictToTest, kwargs):
    """Default retrieve_value on absent key returns ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.get_item_if(
        "missing", condition=ETAG_HAS_CHANGED, expected_etag="fake")

    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


# ── set_item_if default ────────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_default_skips_value_on_failure_when_etag_matches(
        tmpdir, DictToTest, kwargs):
    """On condition failure with matching etag, default skips value fetch."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value="new", condition=ETAG_HAS_CHANGED, expected_etag=etag)

    assert not result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_default_fetches_value_on_failure_when_etag_differs(
        tmpdir, DictToTest, kwargs):
    """On condition failure with differing etag, default fetches value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    old_etag = d.etag("k")

    time.sleep(1.1)
    d["k"] = "modified"

    result = d.set_item_if(
        "k", value="should_not_set",
        condition=ETAG_IS_THE_SAME, expected_etag=old_etag)

    assert not result.condition_was_satisfied
    assert result.new_value == "modified"
    assert d["k"] == "modified"


# ── setdefault_if default ──────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_default_skips_value_when_key_exists_etag_matches(
        tmpdir, DictToTest, kwargs):
    """Key exists, etag matches: default skips value fetch."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "existing"
    etag = d.etag("k")

    result = d.setdefault_if(
        "k", default_value="default",
        condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "existing"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_default_fetches_value_when_key_exists_etag_differs(
        tmpdir, DictToTest, kwargs):
    """Key exists, etag differs: default fetches value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "existing"

    result = d.setdefault_if(
        "k", default_value="default",
        condition=ETAG_HAS_CHANGED, expected_etag="stale_etag")

    assert result.condition_was_satisfied
    assert result.new_value == "existing"
    assert d["k"] == "existing"


# ── get_with_etag always retrieves ─────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_always_retrieves_value(tmpdir, DictToTest, kwargs):
    """get_with_etag always fetches the value despite the default change."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"

    result = d.get_with_etag("k")

    assert result.condition_was_satisfied
    assert result.new_value == "hello"
    assert result.new_value is not VALUE_NOT_RETRIEVED
