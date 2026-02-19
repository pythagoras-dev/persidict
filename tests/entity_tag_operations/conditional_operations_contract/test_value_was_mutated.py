"""Tests for ConditionalOperationResult.value_was_mutated property."""

import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE, KEEP_CURRENT, DELETE_CURRENT,
    ETAG_IS_THE_SAME,
    ConditionalOperationResult,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


# --- Unit tests on the dataclass itself (no backend needed) ---


def test_value_was_mutated_true_when_etags_differ():
    """Property is True when resulting_etag differs from actual_etag."""
    r = ConditionalOperationResult(
        condition_was_satisfied=True,
        actual_etag="etag_v1",
        resulting_etag="etag_v2",
        new_value="hello",
    )
    assert r.value_was_mutated is True


def test_value_was_mutated_false_when_etags_equal():
    """Property is False when resulting_etag equals actual_etag."""
    r = ConditionalOperationResult(
        condition_was_satisfied=True,
        actual_etag="etag_v1",
        resulting_etag="etag_v1",
        new_value="hello",
    )
    assert r.value_was_mutated is False


def test_value_was_mutated_true_for_new_key():
    """Property is True when actual_etag is ITEM_NOT_AVAILABLE (new key created)."""
    r = ConditionalOperationResult(
        condition_was_satisfied=True,
        actual_etag=ITEM_NOT_AVAILABLE,
        resulting_etag="etag_v1",
        new_value="hello",
    )
    assert r.value_was_mutated is True


def test_value_was_mutated_true_for_deletion():
    """Property is True when resulting_etag is ITEM_NOT_AVAILABLE (key deleted)."""
    r = ConditionalOperationResult(
        condition_was_satisfied=True,
        actual_etag="etag_v1",
        resulting_etag=ITEM_NOT_AVAILABLE,
        new_value=ITEM_NOT_AVAILABLE,
    )
    assert r.value_was_mutated is True


def test_value_was_mutated_false_for_missing_key():
    """Property is False when both etags are ITEM_NOT_AVAILABLE (key was and remains absent)."""
    r = ConditionalOperationResult(
        condition_was_satisfied=False,
        actual_etag=ITEM_NOT_AVAILABLE,
        resulting_etag=ITEM_NOT_AVAILABLE,
        new_value=ITEM_NOT_AVAILABLE,
    )
    assert r.value_was_mutated is False


# --- Integration tests through real backend operations ---


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_successful_write_shows_mutated(tmpdir, DictToTest, kwargs):
    """Successful conditional write reports value_was_mutated as True."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.set_item_if("k", value="v2", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is True


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_failed_condition_shows_not_mutated(tmpdir, DictToTest, kwargs):
    """Failed conditional write reports value_was_mutated as False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"

    result = d.set_item_if("k", value="v2", condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")

    assert not result.condition_was_satisfied
    assert result.value_was_mutated is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_shows_not_mutated(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT with matching etag reports value_was_mutated as False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.set_item_if("k", value=KEEP_CURRENT, condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_shows_mutated(tmpdir, DictToTest, kwargs):
    """DELETE_CURRENT with matching etag reports value_was_mutated as True."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.set_item_if("k", value=DELETE_CURRENT, condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is True
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_shows_not_mutated(tmpdir, DictToTest, kwargs):
    """Read-only get_item_if reports value_was_mutated as False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_with_etag_shows_not_mutated(tmpdir, DictToTest, kwargs):
    """Read-only get_with_etag reports value_was_mutated as False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"

    result = d.get_with_etag("k")

    assert result.value_was_mutated is False


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_successful_shows_mutated(tmpdir, DictToTest, kwargs):
    """Successful conditional discard reports value_was_mutated as True."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is True


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_on_missing_key_shows_mutated(tmpdir, DictToTest, kwargs):
    """setdefault_if creating a new key reports value_was_mutated as True."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.setdefault_if("k", default_value="default", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is True
    assert d["k"] == "default"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_on_existing_key_shows_not_mutated(tmpdir, DictToTest, kwargs):
    """setdefault_if on an existing key reports value_was_mutated as False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "existing"
    etag = d.etag("k")

    result = d.setdefault_if("k", default_value="default", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated is False
    assert d["k"] == "existing"
