"""Tests for setdefault_if method."""

import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    DELETE_CURRENT,
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT,
    ItemNotAvailableFlag,
)

from tests.data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_inserts_when_absent_and_condition_satisfied(
        tmpdir, DictToTest, kwargs):
    """Verify setdefault_if inserts when key is missing and condition passes."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault_if("key1", "value", ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert d["key1"] == "value"
    assert not isinstance(result.resulting_etag, ItemNotAvailableFlag)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_noop_when_key_exists_even_if_condition_satisfied(
        tmpdir, DictToTest, kwargs):
    """Verify setdefault_if does not overwrite existing keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["key1"] = "original"
    etag = d.etag("key1")

    result = d.setdefault_if("key1", "new_value", ETAG_IS_THE_SAME, etag)

    assert result.condition_was_satisfied
    assert result.resulting_etag == etag
    assert d["key1"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_missing_key_condition_not_satisfied(
        tmpdir, DictToTest, kwargs):
    """Verify setdefault_if does not insert when condition fails."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault_if("key1", "value", ETAG_HAS_CHANGED, ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "key1" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@pytest.mark.parametrize("joker", [KEEP_CURRENT, DELETE_CURRENT])
@mock_aws
def test_setdefault_if_rejects_jokers(tmpdir, DictToTest, kwargs, joker):
    """Verify setdefault_if rejects joker values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises(TypeError):
        d.setdefault_if("key1", joker, ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE)

    assert "key1" not in d
