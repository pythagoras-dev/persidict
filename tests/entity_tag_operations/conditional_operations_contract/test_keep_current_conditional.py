"""Tests for KEEP_CURRENT across all conditional methods and dict classes.

Exercises KEEP_CURRENT in set_item_if, setdefault_if, transform_item, and
__setitem__ for all mutable backends, plus EmptyDict, WriteOnceDict, and
AppendOnlyDictCached. Focuses on result-field correctness (condition_was_satisfied,
value_was_mutated, actual_etag, resulting_etag, new_value) and observable
state invariants (value unchanged, etag unchanged, key presence).
"""

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict
from persidict.empty_dict import EmptyDict
from persidict.write_once_dict import WriteOnceDict
from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.cached_mutable_dict import MutableDictCached
from persidict.exceptions import MutationPolicyError
from persidict.jokers_and_status_flags import (
    KEEP_CURRENT,
    ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED,
    ITEM_NOT_AVAILABLE, VALUE_NOT_RETRIEVED,
    ALWAYS_RETRIEVE, NEVER_RETRIEVE,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


# ── set_item_if: result-field invariants ────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_any_etag_present_key_result_fields(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ANY_ETAG on existing key: condition satisfied, no mutation."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag == etag
    assert result.resulting_etag == etag
    assert result.new_value == "hello"
    assert d["k"] == "hello"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_any_etag_missing_key_result_fields(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ANY_ETAG on absent key: condition satisfied, key stays absent."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "missing", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert not result.value_was_mutated
    assert "missing" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_etag_same_satisfied_result_fields(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ETAG_IS_THE_SAME with matching etag: full result check."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = [1, 2, 3]
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.actual_etag == etag
    assert result.resulting_etag == etag
    assert result.new_value == [1, 2, 3]


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_etag_same_not_satisfied_result_fields(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ETAG_IS_THE_SAME with wrong etag: condition fails."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag",
        retrieve_value=ALWAYS_RETRIEVE)

    assert not result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value == "value"
    assert d["k"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_etag_changed_on_missing_key_with_real_etag(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ETAG_HAS_CHANGED on absent key with real expected_etag.

    actual_etag is ITEM_NOT_AVAILABLE, which differs from a real etag,
    so condition is satisfied. But key is absent, so result is item_not_available.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "gone", value=KEEP_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag="some_old_etag",
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_etag_changed_on_missing_key_with_item_not_available(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ETAG_HAS_CHANGED + expected=ITEM_NOT_AVAILABLE on absent key.

    Both actual and expected are ITEM_NOT_AVAILABLE → they match → condition
    NOT satisfied (etag has NOT changed).
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "gone", value=KEEP_CURRENT,
        condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_etag_same_on_missing_key_with_item_not_available(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT + ETAG_IS_THE_SAME + expected=ITEM_NOT_AVAILABLE on absent key.

    Both actual and expected are ITEM_NOT_AVAILABLE → they match → condition
    satisfied. Key is absent, so result is item_not_available with satisfied=True.
    """
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.set_item_if(
        "gone", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


# ── set_item_if: observable side-effect invariants ──────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_never_writes_value_even_with_any_etag(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT must never actually write; verify etag+value are untouched."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = {"nested": True}
    etag_before = d.etag("k")

    d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)

    assert d.etag("k") == etag_before
    assert d["k"] == {"nested": True}
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_never_creates_absent_key(tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT on absent key must not create the key, even with ANY_ETAG."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    d.set_item_if(
        "new", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)

    assert "new" not in d
    assert len(d) == 0


# ── set_item_if: retrieve_value interaction ────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_condition_not_satisfied_never_retrieve(
        tmpdir, DictToTest, kwargs):
    """When condition fails, NEVER_RETRIEVE still returns VALUE_NOT_RETRIEVED."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="wrong",
        retrieve_value=NEVER_RETRIEVE)

    assert not result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current_condition_not_satisfied_always_retrieve(
        tmpdir, DictToTest, kwargs):
    """When condition fails, ALWAYS_RETRIEVE still returns the existing value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="wrong",
        retrieve_value=ALWAYS_RETRIEVE)

    assert not result.condition_was_satisfied
    assert result.new_value == "hello"


# ── setdefault_if: rejects KEEP_CURRENT ────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_rejects_keep_current(tmpdir, DictToTest, kwargs):
    """setdefault_if must raise TypeError when default_value is KEEP_CURRENT."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    with pytest.raises(TypeError):
        d.setdefault_if(
            "k", default_value=KEEP_CURRENT,
            condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)


# ── transform_item: KEEP_CURRENT from transformer ──────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_keep_current_result_fields(tmpdir, DictToTest, kwargs):
    """transform_item with KEEP_CURRENT: resulting_etag==actual, value preserved."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    etag_before = d.etag("k")

    result = d.transform_item("k", transformer=lambda v: KEEP_CURRENT)

    assert result.new_value == "original"
    assert result.resulting_etag == etag_before
    assert d["k"] == "original"
    assert d.etag("k") == etag_before


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_keep_current_missing_key_result_fields(tmpdir, DictToTest, kwargs):
    """transform_item with KEEP_CURRENT on absent key: ITEM_NOT_AVAILABLE."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    result = d.transform_item("absent", transformer=lambda v: KEEP_CURRENT)

    assert result.new_value is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert "absent" not in d


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_keep_current_does_not_call_set_or_discard(
        tmpdir, DictToTest, kwargs):
    """KEEP_CURRENT shortcircuits: no write, etag unchanged, value unchanged."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "value"
    etag_before = d.etag("k")

    d.transform_item("k", transformer=lambda v: KEEP_CURRENT)

    assert d.etag("k") == etag_before
    assert d["k"] == "value"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_transform_conditional_keep_current(tmpdir, DictToTest, kwargs):
    """Transformer that conditionally returns KEEP_CURRENT based on value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "already_good"

    def keep_if_good(v):
        if v == "already_good":
            return KEEP_CURRENT
        return "replaced"

    result = d.transform_item("k", transformer=keep_if_good)

    assert result.new_value == "already_good"
    assert d["k"] == "already_good"


# ── __setitem__ with KEEP_CURRENT ──────────────────────────────────────


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setitem_keep_current_noop_on_present_key(tmpdir, DictToTest, kwargs):
    """d[key] = KEEP_CURRENT on existing key: no-op, value+etag unchanged."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"
    etag_before = d.etag("k")

    d["k"] = KEEP_CURRENT

    assert d["k"] == "original"
    assert d.etag("k") == etag_before
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setitem_keep_current_noop_on_missing_key(tmpdir, DictToTest, kwargs):
    """d[key] = KEEP_CURRENT on absent key: no-op, key not created."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    d["missing"] = KEEP_CURRENT

    assert "missing" not in d
    assert len(d) == 0


# ── EmptyDict ──────────────────────────────────────────────────────────


def test_empty_dict_set_item_if_keep_current():
    """EmptyDict.set_item_if with KEEP_CURRENT: always absent, evaluate condition."""
    d = EmptyDict()

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert not result.value_was_mutated


def test_empty_dict_set_item_if_keep_current_etag_same_real_etag():
    """EmptyDict.set_item_if with KEEP_CURRENT + ETAG_IS_THE_SAME + real etag.

    Actual is ITEM_NOT_AVAILABLE, expected is a real etag → condition fails.
    """
    d = EmptyDict()

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag="some_etag",
        retrieve_value=ALWAYS_RETRIEVE)

    assert not result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE


def test_empty_dict_setdefault_if_rejects_keep_current():
    """EmptyDict.setdefault_if must reject KEEP_CURRENT as default_value."""
    d = EmptyDict()

    with pytest.raises(TypeError):
        d.setdefault_if(
            "k", default_value=KEEP_CURRENT,
            condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)


def test_empty_dict_setitem_keep_current():
    """EmptyDict.__setitem__ with KEEP_CURRENT: no-op, dict stays empty."""
    d = EmptyDict()

    d["k"] = KEEP_CURRENT

    assert "k" not in d
    assert len(d) == 0


# ── WriteOnceDict ──────────────────────────────────────────────────────


def test_write_once_set_item_if_raises_even_with_keep_current(tmpdir):
    """WriteOnceDict.set_item_if always raises MutationPolicyError, even for KEEP_CURRENT."""
    inner = FileDirDict(base_dir=str(tmpdir), append_only=True)
    d = WriteOnceDict(wrapped_dict=inner)

    with pytest.raises(MutationPolicyError):
        d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)


def test_write_once_setitem_keep_current_noop(tmpdir):
    """WriteOnceDict[k] = KEEP_CURRENT is a no-op; no error, no state change."""
    inner = FileDirDict(base_dir=str(tmpdir), append_only=True)
    d = WriteOnceDict(wrapped_dict=inner)
    d["k"] = "stored"

    d["k"] = KEEP_CURRENT

    assert d["k"] == "stored"


def test_write_once_setitem_keep_current_missing_key_noop(tmpdir):
    """WriteOnceDict[missing] = KEEP_CURRENT: no-op, key stays absent."""
    inner = FileDirDict(base_dir=str(tmpdir), append_only=True)
    d = WriteOnceDict(wrapped_dict=inner)

    d["missing"] = KEEP_CURRENT

    assert "missing" not in d


# ── AppendOnlyDictCached ───────────────────────────────────────────────


def test_append_only_cached_set_item_if_keep_current(tmpdir):
    """AppendOnlyDictCached.set_item_if with KEEP_CURRENT delegates and preserves value."""
    main = FileDirDict(base_dir=str(tmpdir / "main"), append_only=True,
                       serialization_format="json")
    cache = FileDirDict(base_dir=str(tmpdir / "cache"), append_only=True,
                        serialization_format="json")
    d = AppendOnlyDictCached(main_dict=main, data_cache=cache)
    d["k"] = "stored"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value == "stored"
    assert d["k"] == "stored"


def test_append_only_cached_set_item_if_keep_current_missing_key(tmpdir):
    """AppendOnlyDictCached.set_item_if with KEEP_CURRENT on absent key."""
    main = FileDirDict(base_dir=str(tmpdir / "main"), append_only=True,
                       serialization_format="json")
    cache = FileDirDict(base_dir=str(tmpdir / "cache"), append_only=True,
                        serialization_format="json")
    d = AppendOnlyDictCached(main_dict=main, data_cache=cache)

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "k" not in d


def test_append_only_cached_setitem_keep_current_noop(tmpdir):
    """AppendOnlyDictCached[k] = KEEP_CURRENT: no-op."""
    main = FileDirDict(base_dir=str(tmpdir / "main"), append_only=True,
                       serialization_format="json")
    cache = FileDirDict(base_dir=str(tmpdir / "cache"), append_only=True,
                        serialization_format="json")
    d = AppendOnlyDictCached(main_dict=main, data_cache=cache)
    d["k"] = "stored"

    d["k"] = KEEP_CURRENT

    assert d["k"] == "stored"


# ── MutableDictCached ──────────────────────────────────────────────────


@mock_aws
def test_mutable_cached_set_item_if_keep_current(tmpdir):
    """MutableDictCached.set_item_if with KEEP_CURRENT: delegates, caches stay valid."""
    main = BasicS3Dict(bucket_name="mc-main", serialization_format="json")
    dcache = FileDirDict(base_dir=str(tmpdir / "dcache"),
                         serialization_format="json")
    ecache = FileDirDict(base_dir=str(tmpdir / "ecache"),
                         serialization_format="json")
    d = MutableDictCached(main_dict=main, data_cache=dcache, etag_cache=ecache)
    d["k"] = "stored"
    etag = d.etag("k")

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert not result.value_was_mutated
    assert result.new_value == "stored"
    assert d["k"] == "stored"
    assert d.etag("k") == etag


@mock_aws
def test_mutable_cached_set_item_if_keep_current_missing_key(tmpdir):
    """MutableDictCached.set_item_if with KEEP_CURRENT on absent key."""
    main = BasicS3Dict(bucket_name="mc-main2", serialization_format="json")
    dcache = FileDirDict(base_dir=str(tmpdir / "dcache"),
                         serialization_format="json")
    ecache = FileDirDict(base_dir=str(tmpdir / "ecache"),
                         serialization_format="json")
    d = MutableDictCached(main_dict=main, data_cache=dcache, etag_cache=ecache)

    result = d.set_item_if(
        "k", value=KEEP_CURRENT,
        condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE,
        retrieve_value=ALWAYS_RETRIEVE)

    assert result.condition_was_satisfied
    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE
    assert "k" not in d


@mock_aws
def test_mutable_cached_setitem_keep_current(tmpdir):
    """MutableDictCached[k] = KEEP_CURRENT: no-op, caches unchanged."""
    main = BasicS3Dict(bucket_name="mc-main3", serialization_format="json")
    dcache = FileDirDict(base_dir=str(tmpdir / "dcache"),
                         serialization_format="json")
    ecache = FileDirDict(base_dir=str(tmpdir / "ecache"),
                         serialization_format="json")
    d = MutableDictCached(main_dict=main, data_cache=dcache, etag_cache=ecache)
    d["k"] = "stored"
    etag_before = d.etag("k")

    d["k"] = KEEP_CURRENT

    assert d["k"] == "stored"
    assert d.etag("k") == etag_before


@mock_aws
def test_mutable_cached_transform_keep_current(tmpdir):
    """MutableDictCached.transform_item with KEEP_CURRENT: no mutation, caches valid."""
    main = BasicS3Dict(bucket_name="mc-main4", serialization_format="json")
    dcache = FileDirDict(base_dir=str(tmpdir / "dcache"),
                         serialization_format="json")
    ecache = FileDirDict(base_dir=str(tmpdir / "ecache"),
                         serialization_format="json")
    d = MutableDictCached(main_dict=main, data_cache=dcache, etag_cache=ecache)
    d["k"] = "stored"
    etag_before = d.etag("k")

    result = d.transform_item("k", transformer=lambda v: KEEP_CURRENT)

    assert result.new_value == "stored"
    assert result.resulting_etag == etag_before
    assert d["k"] == "stored"
