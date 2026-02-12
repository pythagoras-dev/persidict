"""Tests for the retrieve_value parameter on conditional operations.

Covers three areas:
1. Validation — TypeError for invalid types (bool, str, None).
2. NEVER_RETRIEVE — value is always VALUE_NOT_RETRIEVED.
3. IF_ETAG_CHANGED — value fetched only when expected_etag != actual_etag.
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
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
)

from tests.data_for_mutable_tests import mutable_tests


# ── Group 1: Validation ─────────────────────────────────────────────────


@pytest.mark.parametrize("bad_value", [True, False, "always", None])
@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_rejects_invalid_retrieve_value(
        tmpdir, DictToTest, kwargs, bad_value):
    """get_item_if raises TypeError for non-RetrieveValueFlag values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    etag = d.etag("k")
    with pytest.raises(TypeError, match="retrieve_value must be"):
        d.get_item_if("k", etag, ETAG_IS_THE_SAME, retrieve_value=bad_value)


@pytest.mark.parametrize("bad_value", [True, False, "always", None])
@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_rejects_invalid_retrieve_value(
        tmpdir, DictToTest, kwargs, bad_value):
    """set_item_if raises TypeError for non-RetrieveValueFlag values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    etag = d.etag("k")
    with pytest.raises(TypeError, match="retrieve_value must be"):
        d.set_item_if("k", "new", etag, ETAG_IS_THE_SAME,
                       retrieve_value=bad_value)


@pytest.mark.parametrize("bad_value", [True, False, "always", None])
@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_rejects_invalid_retrieve_value(
        tmpdir, DictToTest, kwargs, bad_value):
    """setdefault_if raises TypeError for non-RetrieveValueFlag values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    with pytest.raises(TypeError, match="retrieve_value must be"):
        d.setdefault_if("k", "v", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME,
                         retrieve_value=bad_value)


# ── Group 2: NEVER_RETRIEVE ─────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_never_retrieve_returns_value_not_retrieved(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE: existing key → VALUE_NOT_RETRIEVED, real etag."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.get_item_if("k", etag, ETAG_IS_THE_SAME,
                            retrieve_value=NEVER_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert isinstance(result.resulting_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_never_retrieve_absent_key(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE: absent key → ITEM_NOT_AVAILABLE."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.get_item_if("missing", "fake_etag", ETAG_HAS_CHANGED,
                            retrieve_value=NEVER_RETRIEVE)

    assert result.actual_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_never_retrieve_on_failure(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE: condition not satisfied → VALUE_NOT_RETRIEVED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    # Condition: ETAG_HAS_CHANGED with current etag → not satisfied
    result = d.set_item_if("k", "new", etag, ETAG_HAS_CHANGED,
                            retrieve_value=NEVER_RETRIEVE)

    assert not result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_never_retrieve_on_success(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE: condition satisfied, write succeeds → new value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    result = d.set_item_if("k", "updated", etag, ETAG_IS_THE_SAME,
                            retrieve_value=NEVER_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.new_value == "updated"
    assert d["k"] == "updated"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_never_retrieve_key_exists(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE: key exists → VALUE_NOT_RETRIEVED, no overwrite."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "existing"
    etag = d.etag("k")

    result = d.setdefault_if("k", "default", etag, ETAG_IS_THE_SAME,
                              retrieve_value=NEVER_RETRIEVE)

    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "existing"


# ── Group 3: IF_ETAG_CHANGED ────────────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_if_etag_changed_skips_when_same(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: expected == actual → VALUE_NOT_RETRIEVED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.get_item_if("k", etag, ETAG_IS_THE_SAME,
                            retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert result.resulting_etag == etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_item_if_if_etag_changed_fetches_when_different(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: expected != actual → fetches value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "original"
    old_etag = d.etag("k")

    time.sleep(1.1)
    d["k"] = "modified"

    result = d.get_item_if("k", old_etag, ETAG_HAS_CHANGED,
                            retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value == "modified"
    assert result.resulting_etag != old_etag


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_if_etag_changed_skips_when_same(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: condition fails, etags equal → VALUE_NOT_RETRIEVED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    # Condition: ETAG_HAS_CHANGED with current etag → not satisfied
    # retrieve_value: IF_ETAG_CHANGED, but expected == actual → skip
    result = d.set_item_if("k", "new", etag, ETAG_HAS_CHANGED,
                            retrieve_value=IF_ETAG_CHANGED)

    assert not result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_if_etag_changed_fetches_when_different(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: condition fails, etags differ → fetches value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "original"
    old_etag = d.etag("k")

    time.sleep(1.1)
    d["k"] = "modified"

    # Condition: ETAG_IS_THE_SAME with old etag → not satisfied
    # retrieve_value: IF_ETAG_CHANGED, expected != actual → fetch
    result = d.set_item_if("k", "should_not_set", old_etag, ETAG_IS_THE_SAME,
                            retrieve_value=IF_ETAG_CHANGED)

    assert not result.condition_was_satisfied
    assert result.new_value == "modified"
    assert d["k"] == "modified"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_if_etag_changed_key_exists_same_etag(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: key exists, expected == actual → VALUE_NOT_RETRIEVED."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "existing"
    etag = d.etag("k")

    result = d.setdefault_if("k", "default", etag, ETAG_IS_THE_SAME,
                              retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert d["k"] == "existing"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_if_etag_changed_key_exists_different_etag(
        tmpdir, DictToTest, kwargs):
    """IF_ETAG_CHANGED: key exists, expected != actual → fetches value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "existing"
    real_etag = d.etag("k")

    result = d.setdefault_if("k", "default", "stale_etag", ETAG_HAS_CHANGED,
                              retrieve_value=IF_ETAG_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value == "existing"
    assert result.resulting_etag == real_etag
    assert d["k"] == "existing"
