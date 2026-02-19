"""Tests for DELETE_CURRENT behavior across all conditional methods and backends.

Covers set_item_if, discard_if, and transform_item with DELETE_CURRENT,
focusing on result fields, retrieve_value interaction, value_was_mutated,
condition evaluation edge cases, and append-only rejection.
"""

import pytest
from moto import mock_aws

from persidict import MutationPolicyError, FileDirDict, LocalDict, BasicS3Dict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE,
    VALUE_NOT_RETRIEVED,
    DELETE_CURRENT,
    ANY_ETAG,
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
    ALWAYS_RETRIEVE,
    NEVER_RETRIEVE,
    IF_ETAG_CHANGED,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


# ---------------------------------------------------------------------------
# set_item_if with DELETE_CURRENT: retrieve_value interaction on failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_failure_never_retrieve(tmpdir, DictToTest, kwargs):
    """When condition fails with DELETE_CURRENT, NEVER_RETRIEVE yields VALUE_NOT_RETRIEVED."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag",
        retrieve_value=NEVER_RETRIEVE)

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_failure_always_retrieve(tmpdir, DictToTest, kwargs):
    """When condition fails with DELETE_CURRENT, ALWAYS_RETRIEVE returns the stored value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "kept_value"

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag",
        retrieve_value=ALWAYS_RETRIEVE)

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value == "kept_value"
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_failure_if_etag_changed_retrieves(tmpdir, DictToTest, kwargs):
    """When condition fails and expected != actual, IF_ETAG_CHANGED retrieves the value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "stored"

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="stale_etag",
        retrieve_value=IF_ETAG_CHANGED)

    assert not result.condition_was_satisfied
    assert result.new_value == "stored"


# ---------------------------------------------------------------------------
# set_item_if with DELETE_CURRENT: success result completeness
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_success_result_with_always_retrieve(tmpdir, DictToTest, kwargs):
    """Successful DELETE_CURRENT: new_value is ITEM_NOT_AVAILABLE regardless of retrieve_value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "doomed"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.value_was_mutated
    assert result.actual_etag == etag
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_success_result_with_never_retrieve(tmpdir, DictToTest, kwargs):
    """Successful DELETE_CURRENT with NEVER_RETRIEVE still reports ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "doomed"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=NEVER_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


# ---------------------------------------------------------------------------
# set_item_if with DELETE_CURRENT: ETAG_HAS_CHANGED + ITEM_NOT_AVAILABLE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_etag_changed_with_ina_on_existing(tmpdir, DictToTest, kwargs):
    """ETAG_HAS_CHANGED + ITEM_NOT_AVAILABLE on existing key: condition satisfied, key deleted.

    Caller believes key absent but it exists, so actual != expected => satisfied.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "surprise"
    pre_etag = d.etag("k")

    result = d.set_item_if(
        "k", value=DELETE_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert result.value_was_mutated
    assert result.actual_etag == pre_etag
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_etag_changed_with_ina_on_missing(tmpdir, DictToTest, kwargs):
    """ETAG_HAS_CHANGED + ITEM_NOT_AVAILABLE on missing key: condition not satisfied.

    Both expected and actual are ITEM_NOT_AVAILABLE, so ETAG_HAS_CHANGED is False.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "absent", value=DELETE_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE


# ---------------------------------------------------------------------------
# set_item_if with DELETE_CURRENT on missing key: value_was_mutated
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_missing_key_not_mutated(tmpdir, DictToTest, kwargs):
    """DELETE_CURRENT on absent key with satisfied condition: no mutation occurred."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "absent", value=DELETE_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_any_etag_missing_key_not_mutated(tmpdir, DictToTest, kwargs):
    """DELETE_CURRENT + ANY_ETAG on absent key: satisfied but no mutation."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "absent", value=DELETE_CURRENT,
        condition=ANY_ETAG, expected_etag="irrelevant")

    assert result.condition_was_satisfied
    assert not result.value_was_mutated


# ---------------------------------------------------------------------------
# discard_if: thorough result field verification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_success_result_fields(tmpdir, DictToTest, kwargs):
    """Successful discard_if sets resulting_etag=ITEM_NOT_AVAILABLE, new_value=ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"
    etag = d.etag("k")

    result = d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    assert result.condition_was_satisfied
    assert result.value_was_mutated
    assert result.actual_etag == etag
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_failure_result_fields(tmpdir, DictToTest, kwargs):
    """Failed discard_if: new_value is VALUE_NOT_RETRIEVED, etags unchanged."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "protected"
    actual_etag = d.etag("k")

    result = d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag="wrong")

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag == actual_etag
    assert result.resulting_etag == actual_etag
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_missing_key_result_fields(tmpdir, DictToTest, kwargs):
    """discard_if on absent key with satisfied condition: both etags ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_if(
        "absent", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_missing_key_unsatisfied_result_fields(tmpdir, DictToTest, kwargs):
    """discard_if on absent key with failed condition: condition_was_satisfied=False."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_if(
        "absent", condition=ETAG_IS_THE_SAME, expected_etag="some_etag")

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_any_etag_deletes_existing(tmpdir, DictToTest, kwargs):
    """discard_if with ANY_ETAG on existing key: unconditionally deletes."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"

    result = d.discard_if("k", condition=ANY_ETAG, expected_etag="ignored")

    assert result.condition_was_satisfied
    assert result.value_was_mutated
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_any_etag_missing_key(tmpdir, DictToTest, kwargs):
    """discard_if with ANY_ETAG on absent key: satisfied, no mutation."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.discard_if("absent", condition=ANY_ETAG, expected_etag="ignored")

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard_if_etag_changed_ina_on_existing_deletes(tmpdir, DictToTest, kwargs):
    """discard_if ETAG_HAS_CHANGED + ITEM_NOT_AVAILABLE on existing key: deletes."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"

    result = d.discard_if(
        "k", condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert result.condition_was_satisfied
    assert result.value_was_mutated
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "k" not in d


# ---------------------------------------------------------------------------
# transform_item returning DELETE_CURRENT: result fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_delete_current_result_fields_existing_key(tmpdir, DictToTest, kwargs):
    """transform_item with DELETE_CURRENT on existing key: resulting_etag and new_value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "to_delete"

    result = d.transform_item("k", transformer=lambda v: DELETE_CURRENT)

    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_delete_current_receives_actual_value(tmpdir, DictToTest, kwargs):
    """Transformer receives the stored value before returning DELETE_CURRENT."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = {"data": 42}
    received = []

    def capture_then_delete(v):
        received.append(v)
        return DELETE_CURRENT

    d.transform_item("k", transformer=capture_then_delete)

    assert len(received) == 1
    assert received[0] == {"data": 42}
    assert "k" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_delete_current_missing_key_receives_ina(tmpdir, DictToTest, kwargs):
    """Transformer receives ITEM_NOT_AVAILABLE for missing key, returns DELETE_CURRENT."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    received = []

    def capture_then_delete(v):
        received.append(v)
        return DELETE_CURRENT

    result = d.transform_item("absent", transformer=capture_then_delete)

    assert len(received) == 1
    assert received[0] is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


# ---------------------------------------------------------------------------
# Append-only: set_item_if with DELETE_CURRENT
# ---------------------------------------------------------------------------


append_only_tests = [
    (FileDirDict, dict(serialization_format="json", append_only=True)),
    (LocalDict, dict(serialization_format="json", bucket_name="ao_bucket",
                     append_only=True)),
    (BasicS3Dict, dict(serialization_format="json", bucket_name="ao_bucket",
                       append_only=True)),
    (S3Dict_FileDirCached, dict(serialization_format="json",
                                bucket_name="ao_bucket", append_only=True)),
]


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@pytest.mark.parametrize("condition", [ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED])
@mock_aws
def test_set_item_if_delete_current_on_append_only_raises(tmpdir, DictToTest, kwargs, condition):
    """set_item_if(value=DELETE_CURRENT) on append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    etag = d.etag("k")

    expected_etag_map = {
        ANY_ETAG: ITEM_NOT_AVAILABLE,
        ETAG_IS_THE_SAME: etag,
        ETAG_HAS_CHANGED: "bogus",
    }

    with pytest.raises(MutationPolicyError):
        d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=condition, expected_etag=expected_etag_map[condition])
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_setitem_delete_current_on_append_only_raises(tmpdir, DictToTest, kwargs):
    """d[key] = DELETE_CURRENT on append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"

    with pytest.raises(MutationPolicyError):
        d["k"] = DELETE_CURRENT
    assert "k" in d


# ---------------------------------------------------------------------------
# set_item_if + DELETE_CURRENT: condition failure on missing key
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_set_item_if_delete_current_missing_key_failure_result(tmpdir, DictToTest, kwargs):
    """DELETE_CURRENT on missing key with unsatisfied condition: key stays absent."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "absent", value=DELETE_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "absent" not in d


# ---------------------------------------------------------------------------
# set_item_if + DELETE_CURRENT: key removal is observable in len/keys/iter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_via_set_item_if_observable_in_len_and_keys(tmpdir, DictToTest, kwargs):
    """After DELETE_CURRENT via set_item_if, key is absent from len, keys, and iteration."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["a"] = 1
    d["b"] = 2
    d["c"] = 3
    etag_b = d.etag("b")

    d.set_item_if("b", value=DELETE_CURRENT,
                   condition=ETAG_IS_THE_SAME, expected_etag=etag_b)

    assert len(d) == 2
    assert "b" not in d
    assert set(d.keys()) == {("a",), ("c",)}


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current_via_discard_if_observable_in_len_and_keys(tmpdir, DictToTest, kwargs):
    """After discard_if, key is absent from len, keys, and iteration."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["a"] = 1
    d["b"] = 2
    d["c"] = 3
    etag_b = d.etag("b")

    d.discard_if("b", condition=ETAG_IS_THE_SAME, expected_etag=etag_b)

    assert len(d) == 2
    assert "b" not in d
    assert set(d.keys()) == {("a",), ("c",)}


# ---------------------------------------------------------------------------
# set_item_if + DELETE_CURRENT: verify etag gone after delete
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_raises_after_delete_current_via_set_item_if(tmpdir, DictToTest, kwargs):
    """After DELETE_CURRENT, etag() raises KeyError (key no longer exists)."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"
    etag = d.etag("k")

    d.set_item_if("k", value=DELETE_CURRENT,
                   condition=ETAG_IS_THE_SAME, expected_etag=etag)

    with pytest.raises(KeyError):
        d.etag("k")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_etag_raises_after_discard_if(tmpdir, DictToTest, kwargs):
    """After discard_if, etag() raises KeyError (key no longer exists)."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"
    etag = d.etag("k")

    d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    with pytest.raises(KeyError):
        d.etag("k")


# ---------------------------------------------------------------------------
# Re-create after DELETE_CURRENT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_recreate_after_delete_current_via_set_item_if(tmpdir, DictToTest, kwargs):
    """A key deleted by DELETE_CURRENT can be re-created with a new value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    d.set_item_if("k", value=DELETE_CURRENT,
                   condition=ETAG_IS_THE_SAME, expected_etag=etag)
    assert "k" not in d

    d["k"] = "reborn"
    assert d["k"] == "reborn"
    assert d.etag("k") is not ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_recreate_after_discard_if(tmpdir, DictToTest, kwargs):
    """A key deleted by discard_if can be re-created."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    etag = d.etag("k")

    d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    assert "k" not in d

    d["k"] = "reborn"
    assert d["k"] == "reborn"


# ---------------------------------------------------------------------------
# DELETE_CURRENT via transform_item on append-only dict
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_transform_delete_current_on_append_only_raises(tmpdir, DictToTest, kwargs):
    """transform_item returning DELETE_CURRENT on append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"

    with pytest.raises(MutationPolicyError):
        d.transform_item("k", transformer=lambda v: DELETE_CURRENT)
    assert "k" in d
