"""Tests for expected_etag=ITEM_NOT_AVAILABLE across all conditional methods and classes.

When expected_etag is ITEM_NOT_AVAILABLE, the caller believes the key is absent.
Condition semantics:
    - ETAG_IS_THE_SAME: satisfied only when key IS actually absent (INA == INA)
    - ETAG_HAS_CHANGED: satisfied only when key actually EXISTS (INA != real_etag)
    - ANY_ETAG: always satisfied

Covers set_item_if, get_item_if, setdefault_if, and discard_if for every concrete
PersiDict implementation. Exercises joker values (KEEP_CURRENT, DELETE_CURRENT),
retrieve_value interactions, result-field correctness, and observable side effects.
"""
from __future__ import annotations

from contextlib import contextmanager

import pytest
from moto import mock_aws

from persidict import (
    BasicS3Dict,
    FileDirDict,
    LocalDict,
    S3Dict_FileDirCached,
    ANY_ETAG,
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
    VALUE_NOT_RETRIEVED,
)
from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.cached_mutable_dict import MutableDictCached
from persidict.empty_dict import EmptyDict
from persidict.exceptions import MutationPolicyError
from persidict.jokers_and_status_flags import (
    KEEP_CURRENT,
    DELETE_CURRENT,
    ALWAYS_RETRIEVE,
    NEVER_RETRIEVE,
)
from persidict.write_once_dict import WriteOnceDict


# ── Fixtures ──────────────────────────────────────────────────────────


@contextmanager
def maybe_mock_aws(enabled: bool):
    if enabled:
        with mock_aws():
            yield
    else:
        yield


def _build_local(_: object) -> LocalDict:
    return LocalDict(serialization_format="json")


def _build_file(tmp_path) -> FileDirDict:
    return FileDirDict(
        base_dir=str(tmp_path / "file"), serialization_format="json")


def _build_basic_s3(_: object) -> BasicS3Dict:
    return BasicS3Dict(
        bucket_name="ina-expected-basic", serialization_format="json")


def _build_s3_cached(tmp_path) -> S3Dict_FileDirCached:
    return S3Dict_FileDirCached(
        bucket_name="ina-expected-cached",
        base_dir=str(tmp_path / "s3-cache"),
        serialization_format="json",
    )


def _build_mutable_cached(_: object) -> MutableDictCached:
    main = LocalDict(serialization_format="json")
    data_cache = LocalDict(serialization_format="pkl")
    etag_cache = LocalDict(serialization_format="json")
    return MutableDictCached(
        main_dict=main, data_cache=data_cache, etag_cache=etag_cache)


STANDARD_SPECS = [
    dict(name="local", uses_s3=False, factory=_build_local),
    dict(name="file", uses_s3=False, factory=_build_file),
    dict(name="basic_s3", uses_s3=True, factory=_build_basic_s3),
    dict(name="s3_cached", uses_s3=True, factory=_build_s3_cached),
    dict(name="mutable_cached", uses_s3=False, factory=_build_mutable_cached),
]


# ═══════════════════════════════════════════════════════════════════════
# set_item_if  (expected_etag=ITEM_NOT_AVAILABLE)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetItemIfItemNotAvailable:

    # -- Absent key scenarios --

    def test_absent_key_etag_is_the_same_inserts(self, tmp_path, spec):
        """INA == INA → satisfied, value written."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value="v1",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "v1"
            assert d["k"] == "v1"
            assert len(d) == 1

    def test_absent_key_etag_has_changed_blocks_write(self, tmp_path, spec):
        """INA != INA is False → not satisfied, no write."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value="v1",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_absent_key_any_etag_inserts(self, tmp_path, spec):
        """ANY_ETAG always satisfied → value written."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value="v1",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert d["k"] == "v1"

    # -- Existing key scenarios --

    def test_existing_key_etag_is_the_same_blocks_write(
            self, tmp_path, spec):
        """INA != real_etag → not satisfied, original value preserved."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_etag_is_the_same_returns_current_value(
            self, tmp_path, spec):
        """Blocked write with default retrieve returns current value
        (expected != actual triggers fetch)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.new_value == "existing"

    def test_existing_key_etag_has_changed_overwrites(
            self, tmp_path, spec):
        """INA != real_etag → satisfied, value overwritten."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "old"
            etag_before = d.etag("k")

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.resulting_etag != etag_before
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "new"
            assert d["k"] == "new"

    def test_existing_key_any_etag_overwrites(self, tmp_path, spec):
        """ANY_ETAG always satisfied → value overwritten."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "old"

            result = d.set_item_if(
                "k", value="new",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert d["k"] == "new"

    # -- KEEP_CURRENT joker --

    def test_keep_current_absent_key_etag_is_the_same(
            self, tmp_path, spec):
        """KEEP_CURRENT on absent key, satisfied: key stays absent."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_keep_current_absent_key_etag_has_changed(
            self, tmp_path, spec):
        """KEEP_CURRENT on absent key, ETAG_HAS_CHANGED: not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_keep_current_existing_key_etag_has_changed(
            self, tmp_path, spec):
        """KEEP_CURRENT on existing key, ETAG_HAS_CHANGED: satisfied,
        no mutation, returns current value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"

    # -- DELETE_CURRENT joker --

    def test_delete_current_absent_key_etag_is_the_same(
            self, tmp_path, spec):
        """DELETE_CURRENT on absent key, satisfied: nothing to delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_absent_key_etag_has_changed(
            self, tmp_path, spec):
        """DELETE_CURRENT on absent key, ETAG_HAS_CHANGED: not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_existing_key_etag_has_changed(
            self, tmp_path, spec):
        """DELETE_CURRENT on existing key, ETAG_HAS_CHANGED: satisfied,
        key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag_before = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag_before
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d
            assert len(d) == 0

    # -- retrieve_value interactions --

    def test_absent_key_etag_is_the_same_never_retrieve(
            self, tmp_path, spec):
        """Successful insert + NEVER_RETRIEVE: new_value is written value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value="v1",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=NEVER_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.new_value == "v1"

    def test_existing_key_etag_is_the_same_never_retrieve(
            self, tmp_path, spec):
        """Blocked write + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "existing"

    def test_existing_key_etag_is_the_same_always_retrieve(
            self, tmp_path, spec):
        """Blocked write + ALWAYS_RETRIEVE: returns existing value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value == "existing"


# ═══════════════════════════════════════════════════════════════════════
# get_item_if  (expected_etag=ITEM_NOT_AVAILABLE)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestGetItemIfItemNotAvailable:

    # -- Absent key scenarios --

    def test_absent_key_etag_is_the_same_satisfied(self, tmp_path, spec):
        """INA == INA → satisfied, all-INA result."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_absent_key_etag_has_changed_not_satisfied(
            self, tmp_path, spec):
        """INA != INA is False → not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_absent_key_any_etag_satisfied(self, tmp_path, spec):
        """ANY_ETAG always satisfied, absent key returns INA."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    # -- Existing key scenarios --

    def test_existing_key_etag_is_the_same_not_satisfied(
            self, tmp_path, spec):
        """INA != real_etag → not satisfied, returns value (default
        retrieve: expected != actual triggers fetch)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "val"

    def test_existing_key_etag_has_changed_satisfied(
            self, tmp_path, spec):
        """INA != real_etag → satisfied, returns value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "val"

    def test_existing_key_any_etag_satisfied(self, tmp_path, spec):
        """ANY_ETAG always satisfied, returns value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"

            result = d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.new_value == "val"

    # -- retrieve_value interactions --

    def test_existing_key_etag_is_the_same_never_retrieve(
            self, tmp_path, spec):
        """Blocked + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_no_mutation_on_dict(self, tmp_path, spec):
        """get_item_if never mutates the dict regardless of condition."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"
            etag = d.etag("k")

            d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)
            d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)
            d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert d["k"] == "val"
            assert d.etag("k") == etag
            assert len(d) == 1


# ═══════════════════════════════════════════════════════════════════════
# setdefault_if  (expected_etag=ITEM_NOT_AVAILABLE)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetdefaultIfItemNotAvailable:

    # -- Absent key scenarios --

    def test_absent_key_etag_is_the_same_inserts_default(
            self, tmp_path, spec):
        """INA == INA → satisfied, default_value inserted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "default"
            assert d["k"] == "default"
            assert len(d) == 1

    def test_absent_key_etag_has_changed_no_insert(self, tmp_path, spec):
        """INA != INA is False → not satisfied, no insert."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_absent_key_any_etag_inserts(self, tmp_path, spec):
        """ANY_ETAG always satisfied → default_value inserted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert d["k"] == "default"

    # -- Existing key scenarios --

    def test_existing_key_etag_is_the_same_not_satisfied_no_overwrite(
            self, tmp_path, spec):
        """INA != real_etag → not satisfied, no overwrite."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_etag_has_changed_satisfied_no_overwrite(
            self, tmp_path, spec):
        """INA != real_etag → satisfied, but setdefault never overwrites
        existing keys."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_etag_has_changed_returns_existing_value(
            self, tmp_path, spec):
        """Satisfied setdefault on existing key returns existing value
        (default retrieve: expected != actual triggers fetch)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.new_value == "existing"

    def test_existing_key_any_etag_no_overwrite(self, tmp_path, spec):
        """ANY_ETAG satisfied, but setdefault never overwrites."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert d["k"] == "existing"

    def test_existing_key_etag_has_changed_never_retrieve(
            self, tmp_path, spec):
        """Existing key + ETAG_HAS_CHANGED + NEVER_RETRIEVE:
        VALUE_NOT_RETRIEVED, no overwrite."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=NEVER_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "existing"


# ═══════════════════════════════════════════════════════════════════════
# discard_if  (expected_etag=ITEM_NOT_AVAILABLE)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestDiscardIfItemNotAvailable:

    # -- Absent key scenarios --

    def test_absent_key_etag_is_the_same_satisfied_noop(
            self, tmp_path, spec):
        """INA == INA → satisfied, but nothing to delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_absent_key_etag_has_changed_not_satisfied(
            self, tmp_path, spec):
        """INA != INA is False → not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_absent_key_any_etag_satisfied_noop(self, tmp_path, spec):
        """ANY_ETAG satisfied, but nothing to delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.discard_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    # -- Existing key scenarios --

    def test_existing_key_etag_is_the_same_not_satisfied(
            self, tmp_path, spec):
        """INA != real_etag → not satisfied, key survives."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "survivor"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "survivor"

    def test_existing_key_etag_has_changed_deletes(self, tmp_path, spec):
        """INA != real_etag → satisfied, key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag_before = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag_before
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_existing_key_any_etag_deletes(self, tmp_path, spec):
        """ANY_ETAG satisfied → key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"

            result = d.discard_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert "k" not in d

    def test_observable_side_effects_after_delete(self, tmp_path, spec):
        """After successful discard_if: len, contains, etag all reflect
        absence."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "val"

            d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert len(d) == 0
            assert "k" not in d
            with pytest.raises(KeyError):
                d.etag("k")


# ═══════════════════════════════════════════════════════════════════════
# EmptyDict
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyDictItemNotAvailable:
    """EmptyDict: key is always absent. When expected_etag=ITEM_NOT_AVAILABLE,
    ETAG_IS_THE_SAME and ANY_ETAG are satisfied; ETAG_HAS_CHANGED is not."""

    def test_get_item_if_etag_is_the_same_satisfied(self):
        """INA == INA → satisfied, all-INA result."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_etag_has_changed_not_satisfied(self):
        """INA != INA is False → not satisfied."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_get_item_if_any_etag_satisfied(self):
        """ANY_ETAG always satisfied."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_etag_is_the_same_satisfied_discards(self):
        """Satisfied but write silently discarded; value_was_mutated=False."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_set_item_if_etag_has_changed_not_satisfied(self):
        """ETAG_HAS_CHANGED not satisfied."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_set_item_if_any_etag_satisfied_discards(self):
        """ANY_ETAG satisfied but write discarded; value_was_mutated=False."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_keep_current(self):
        """KEEP_CURRENT on EmptyDict: satisfied, INA everywhere."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_delete_current(self):
        """DELETE_CURRENT on EmptyDict: satisfied, nothing to delete."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_setdefault_if_etag_is_the_same_satisfied_discards(self):
        """Satisfied but insert discarded; value_was_mutated=False."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_setdefault_if_etag_has_changed_not_satisfied(self):
        """ETAG_HAS_CHANGED not satisfied."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_discard_if_etag_is_the_same_satisfied_noop(self):
        """Satisfied, nothing to discard."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_discard_if_etag_has_changed_not_satisfied(self):
        """ETAG_HAS_CHANGED not satisfied."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# WriteOnceDict
# ═══════════════════════════════════════════════════════════════════════


class TestWriteOnceDictItemNotAvailable:

    def test_set_item_if_raises_mutation_policy_error(self, tmp_path):
        """WriteOnceDict.set_item_if always raises MutationPolicyError."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value="val",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_set_item_if_etag_has_changed_also_raises(self, tmp_path):
        """WriteOnceDict.set_item_if raises regardless of condition."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value="val",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_get_item_if_absent_key_etag_is_the_same(self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME: satisfied, INA result."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_existing_key_etag_has_changed(self, tmp_path):
        """Existing key + ETAG_HAS_CHANGED: satisfied, returns value."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_setdefault_if_absent_key_etag_is_the_same_inserts(
            self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME: satisfied, inserts default."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_etag_has_changed_no_overwrite(
            self, tmp_path):
        """Existing key + ETAG_HAS_CHANGED: satisfied, no overwrite."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "existing"

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert d["k"] == "existing"


# ═══════════════════════════════════════════════════════════════════════
# AppendOnlyDictCached
# ═══════════════════════════════════════════════════════════════════════


class TestAppendOnlyDictCachedItemNotAvailable:

    def _make(self, tmp_path) -> AppendOnlyDictCached:
        main = FileDirDict(
            base_dir=str(tmp_path / "main"), append_only=True,
            serialization_format="json")
        cache = FileDirDict(
            base_dir=str(tmp_path / "cache"), append_only=True,
            serialization_format="json")
        return AppendOnlyDictCached(main_dict=main, data_cache=cache)

    def test_get_item_if_absent_key_etag_is_the_same(self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME: satisfied, INA result."""
        d = self._make(tmp_path)

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_existing_key_etag_has_changed(self, tmp_path):
        """Existing key + ETAG_HAS_CHANGED: satisfied, returns value."""
        d = self._make(tmp_path)
        d["k"] = "val"

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_set_item_if_absent_key_etag_is_the_same_inserts(
            self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME: satisfied, inserts value."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "val"

    def test_set_item_if_absent_key_etag_has_changed_blocks(
            self, tmp_path):
        """Absent key + ETAG_HAS_CHANGED: not satisfied, no insert."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert not result.value_was_mutated
        assert "k" not in d

    def test_setdefault_if_absent_key_etag_is_the_same_inserts(
            self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME: satisfied, inserts default."""
        d = self._make(tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_etag_has_changed_no_overwrite(
            self, tmp_path):
        """Existing key + ETAG_HAS_CHANGED: satisfied, no overwrite."""
        d = self._make(tmp_path)
        d["k"] = "existing"

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert d["k"] == "existing"

    def test_discard_if_raises_mutation_policy_error(self, tmp_path):
        """AppendOnlyDictCached.discard_if always raises."""
        d = self._make(tmp_path)

        with pytest.raises(MutationPolicyError):
            d.discard_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)


# ═══════════════════════════════════════════════════════════════════════
# MutableDictCached  (S3-backed main with caches)
# ═══════════════════════════════════════════════════════════════════════


class TestMutableDictCachedS3ItemNotAvailable:

    def _make(self, bucket_name: str, tmp_path) -> MutableDictCached:
        main = BasicS3Dict(
            bucket_name=bucket_name, serialization_format="json")
        dcache = FileDirDict(
            base_dir=str(tmp_path / "dcache"), serialization_format="json")
        ecache = FileDirDict(
            base_dir=str(tmp_path / "ecache"), serialization_format="json")
        return MutableDictCached(
            main_dict=main, data_cache=dcache, etag_cache=ecache)

    @mock_aws
    def test_set_item_if_absent_etag_is_the_same_inserts(self, tmp_path):
        """Insert via ETAG_IS_THE_SAME + INA on S3-backed cache."""
        d = self._make("mc-ina-insert", tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "val"

    @mock_aws
    def test_set_item_if_existing_etag_has_changed_overwrites(
            self, tmp_path):
        """Overwrite via ETAG_HAS_CHANGED + INA on S3-backed cache."""
        d = self._make("mc-ina-overwrite", tmp_path)
        d["k"] = "old"

        result = d.set_item_if(
            "k", value="new",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "new"

    @mock_aws
    def test_set_item_if_existing_etag_is_the_same_blocks(self, tmp_path):
        """INA != real_etag → not satisfied on S3-backed cache."""
        d = self._make("mc-ina-block", tmp_path)
        d["k"] = "existing"

        result = d.set_item_if(
            "k", value="new",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert d["k"] == "existing"

    @mock_aws
    def test_get_item_if_absent_etag_is_the_same(self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME on S3-backed cache."""
        d = self._make("mc-ina-get", tmp_path)

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    @mock_aws
    def test_discard_if_existing_etag_has_changed_deletes(self, tmp_path):
        """Delete via ETAG_HAS_CHANGED + INA on S3-backed cache."""
        d = self._make("mc-ina-discard", tmp_path)
        d["k"] = "doomed"

        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert "k" not in d

    @mock_aws
    def test_discard_if_absent_etag_is_the_same_noop(self, tmp_path):
        """Absent key + ETAG_IS_THE_SAME on S3-backed cache: no-op."""
        d = self._make("mc-ina-discard-noop", tmp_path)

        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated

    @mock_aws
    def test_setdefault_if_absent_etag_is_the_same_inserts(
            self, tmp_path):
        """Insert default via ETAG_IS_THE_SAME + INA on S3-backed cache."""
        d = self._make("mc-ina-setdef", tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    @mock_aws
    def test_set_item_if_keep_current_existing_etag_has_changed(
            self, tmp_path):
        """KEEP_CURRENT + ETAG_HAS_CHANGED + INA on existing key:
        satisfied, no mutation."""
        d = self._make("mc-ina-keep", tmp_path)
        d["k"] = "preserved"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "preserved"
        assert d.etag("k") == etag

    @mock_aws
    def test_set_item_if_delete_current_existing_etag_has_changed(
            self, tmp_path):
        """DELETE_CURRENT + ETAG_HAS_CHANGED + INA on existing key:
        key deleted."""
        d = self._make("mc-ina-del", tmp_path)
        d["k"] = "doomed"

        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert "k" not in d
