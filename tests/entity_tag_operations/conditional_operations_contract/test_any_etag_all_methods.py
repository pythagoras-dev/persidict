"""Comprehensive tests for ANY_ETAG across all conditional methods and classes.

Covers set_item_if, get_item_if, setdefault_if, discard_if for every concrete
PersiDict implementation.  ANY_ETAG always satisfies the condition, so the tests
focus on retrieve_value interactions, joker values (KEEP_CURRENT, DELETE_CURRENT),
and result-field correctness.  Key bug-hunting area: IF_ETAG_CHANGED (default
retrieve_value) still checks expected_etag == actual_etag even when condition is
ANY_ETAG, so the value may or may not be retrieved depending on expected_etag.
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
    return FileDirDict(base_dir=str(tmp_path / "file"), serialization_format="json")


def _build_basic_s3(_: object) -> BasicS3Dict:
    return BasicS3Dict(
        bucket_name="any-etag-all-methods", serialization_format="json")


def _build_s3_cached(tmp_path) -> S3Dict_FileDirCached:
    return S3Dict_FileDirCached(
        bucket_name="any-etag-cached",
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


def mismatched_etag(spec: dict, etag: str) -> str:
    """Produce an ETag that is guaranteed to differ from ``etag``."""
    if spec["uses_s3"]:
        base = str(etag).strip('"')
        return f'"{base}-mismatch"'
    return f"{etag}-mismatch"


# ═══════════════════════════════════════════════════════════════════════
# get_item_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestGetItemIfAnyEtag:

    def test_existing_key_item_not_available_expected_retrieves_value(
            self, tmp_path, spec):
        """ANY_ETAG + ITEM_NOT_AVAILABLE expected on existing key: default
        retrieve fetches value (expected != actual so IF_ETAG_CHANGED fires)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value == "v1"

    def test_existing_key_matching_expected_default_retrieve_skips_value(
            self, tmp_path, spec):
        """ANY_ETAG + matching expected_etag + IF_ETAG_CHANGED (default):
        value is not retrieved because expected == actual."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ANY_ETAG, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_existing_key_mismatched_expected_default_retrieve_gets_value(
            self, tmp_path, spec):
        """ANY_ETAG + mismatched expected_etag + IF_ETAG_CHANGED: value is
        retrieved because expected != actual."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_existing_key_always_retrieve(self, tmp_path, spec):
        """ANY_ETAG + ALWAYS_RETRIEVE: value is always returned."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ANY_ETAG, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_existing_key_never_retrieve(self, tmp_path, spec):
        """ANY_ETAG + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED regardless."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ANY_ETAG, expected_etag=etag,
                retrieve_value=NEVER_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_missing_key_item_not_available(self, tmp_path, spec):
        """ANY_ETAG on missing key + ITEM_NOT_AVAILABLE: satisfied, absent."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_missing_key_real_expected_etag_still_satisfied(
            self, tmp_path, spec):
        """ANY_ETAG on missing key + real expected_etag: satisfied (unlike
        ETAG_IS_THE_SAME which would not be satisfied)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.get_item_if(
                "temp", condition=ANY_ETAG,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_no_mutation_on_dict(self, tmp_path, spec):
        """get_item_if with ANY_ETAG should never mutate the dict."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            d.get_item_if(
                "k", condition=ANY_ETAG, expected_etag=etag)
            d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)
            d.get_item_if(
                "k", condition=ANY_ETAG,
                expected_etag=mismatched_etag(spec, etag))

            assert d["k"] == "v1"
            assert d.etag("k") == etag
            assert len(d) == 1


# ═══════════════════════════════════════════════════════════════════════
# set_item_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetItemIfAnyEtag:

    def test_existing_key_overwrites(self, tmp_path, spec):
        """ANY_ETAG unconditionally overwrites an existing key."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag_before = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag_before
            assert result.resulting_etag != etag_before
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "v2"
            assert d["k"] == "v2"

    def test_missing_key_inserts(self, tmp_path, spec):
        """ANY_ETAG inserts into an empty dict."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value="v1",
                condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "v1"
            assert d["k"] == "v1"

    def test_missing_key_real_expected_etag_still_inserts(
            self, tmp_path, spec):
        """ANY_ETAG + real expected_etag on missing key: still inserts
        (unlike ETAG_IS_THE_SAME which would fail)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.set_item_if(
                "temp", value="new",
                condition=ANY_ETAG, expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert d["temp"] == "new"

    def test_keep_current_matching_expected_default_retrieve(
            self, tmp_path, spec):
        """KEEP_CURRENT + matching expected_etag + default retrieve: no
        mutation, VALUE_NOT_RETRIEVED (expected == actual skips fetch)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ANY_ETAG, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "preserved"
            assert d.etag("k") == etag

    def test_keep_current_matching_expected_always_retrieve(
            self, tmp_path, spec):
        """KEEP_CURRENT + matching expected_etag + ALWAYS_RETRIEVE:
        no mutation, returns the existing value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ANY_ETAG, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "preserved"

    def test_keep_current_mismatched_expected_default_retrieve_returns_value(
            self, tmp_path, spec):
        """KEEP_CURRENT + mismatched expected_etag + default retrieve:
        no mutation, returns existing value (etags differ so fetch fires)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ANY_ETAG,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"

    def test_keep_current_missing_key(self, tmp_path, spec):
        """KEEP_CURRENT + ANY_ETAG on missing key: satisfied, no mutation,
        key stays absent."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_existing_key(self, tmp_path, spec):
        """DELETE_CURRENT + ANY_ETAG on existing key: key is deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_missing_key(self, tmp_path, spec):
        """DELETE_CURRENT + ANY_ETAG on missing key: satisfied, no mutation."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d


# ═══════════════════════════════════════════════════════════════════════
# setdefault_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetdefaultIfAnyEtag:

    def test_missing_key_item_not_available_inserts(self, tmp_path, spec):
        """ANY_ETAG + ITEM_NOT_AVAILABLE on missing key: inserts default."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "default"
            assert d["k"] == "default"

    def test_missing_key_real_expected_etag_still_inserts(
            self, tmp_path, spec):
        """ANY_ETAG + real expected_etag on missing key: still inserts
        (unlike ETAG_IS_THE_SAME which would fail)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.setdefault_if(
                "temp", default_value="default",
                condition=ANY_ETAG, expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert d["temp"] == "default"

    def test_existing_key_matching_expected_default_retrieve_skips_value(
            self, tmp_path, spec):
        """ANY_ETAG + matching expected_etag + default retrieve on existing key:
        no overwrite, VALUE_NOT_RETRIEVED (expected == actual skips fetch)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "existing"

    def test_existing_key_mismatched_expected_default_retrieve_returns_value(
            self, tmp_path, spec):
        """ANY_ETAG + mismatched expected_etag + default retrieve on existing
        key: no overwrite, value is returned (etags differ so fetch fires)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "existing"
            assert d["k"] == "existing"

    def test_existing_key_always_retrieve(self, tmp_path, spec):
        """ANY_ETAG + ALWAYS_RETRIEVE on existing key: returns value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "existing"

    def test_existing_key_item_not_available_expected_satisfied(
            self, tmp_path, spec):
        """ANY_ETAG + ITEM_NOT_AVAILABLE expected on existing key: satisfied,
        no overwrite, value returned (expected != actual so fetch fires)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.new_value == "existing"
            assert d["k"] == "existing"


# ═══════════════════════════════════════════════════════════════════════
# discard_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestDiscardIfAnyEtag:

    def test_existing_key_deletes(self, tmp_path, spec):
        """ANY_ETAG unconditionally deletes an existing key."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ANY_ETAG, expected_etag=etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_existing_key_item_not_available_expected_still_deletes(
            self, tmp_path, spec):
        """ANY_ETAG + ITEM_NOT_AVAILABLE expected on existing key: still
        deletes (unlike ETAG_IS_THE_SAME which would not be satisfied)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_missing_key_item_not_available_satisfied(self, tmp_path, spec):
        """ANY_ETAG on missing key + ITEM_NOT_AVAILABLE: satisfied, no-op."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.discard_if(
                "k", condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_missing_key_real_expected_etag_satisfied(self, tmp_path, spec):
        """ANY_ETAG on missing key + real expected_etag: satisfied (unlike
        ETAG_IS_THE_SAME which would not be satisfied)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.discard_if(
                "temp", condition=ANY_ETAG,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# EmptyDict
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyDictAnyEtag:

    def test_get_item_if_item_not_available_satisfied(self):
        """EmptyDict get_item_if + ANY_ETAG + ITEM_NOT_AVAILABLE: satisfied."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_real_etag_still_satisfied(self):
        """EmptyDict get_item_if + ANY_ETAG + real ETag: still satisfied
        (unlike ETAG_IS_THE_SAME which would not be satisfied)."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ANY_ETAG,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_set_item_if_satisfied_discards(self):
        """EmptyDict set_item_if + ANY_ETAG: satisfied, write discarded."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_setdefault_if_satisfied_discards(self):
        """EmptyDict setdefault_if + ANY_ETAG: satisfied, write discarded."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_discard_if_satisfied_noop(self):
        """EmptyDict discard_if + ANY_ETAG: satisfied, no-op."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_keep_current_satisfied(self):
        """EmptyDict set_item_if KEEP_CURRENT + ANY_ETAG: satisfied,
        key stays absent."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_delete_current_satisfied(self):
        """EmptyDict set_item_if DELETE_CURRENT + ANY_ETAG: satisfied,
        nothing to delete."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# WriteOnceDict
# ═══════════════════════════════════════════════════════════════════════


class TestWriteOnceDictAnyEtag:

    def test_set_item_if_raises_mutation_policy_error(self, tmp_path):
        """WriteOnceDict.set_item_if always raises MutationPolicyError."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value="val",
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_set_item_if_keep_current_also_raises(self, tmp_path):
        """WriteOnceDict.set_item_if raises even with KEEP_CURRENT."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ANY_ETAG,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_get_item_if_delegates_to_wrapped(self, tmp_path):
        """WriteOnceDict.get_item_if delegates and works correctly."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ANY_ETAG, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_get_item_if_missing_key_real_etag_satisfied(self, tmp_path):
        """WriteOnceDict.get_item_if + ANY_ETAG on missing key with real
        expected_etag: satisfied (unlike ETAG_IS_THE_SAME)."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.get_item_if(
            "k", condition=ANY_ETAG,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_setdefault_if_inserts_when_absent(self, tmp_path):
        """WriteOnceDict.setdefault_if + ANY_ETAG inserts when absent."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_no_overwrite(self, tmp_path):
        """WriteOnceDict.setdefault_if + ANY_ETAG on existing key: no overwrite."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ANY_ETAG, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "val"
        assert d["k"] == "val"

    def test_discard_if_missing_key_satisfied_noop(self, tmp_path):
        """WriteOnceDict.discard_if + ANY_ETAG on missing key: satisfied,
        no error (early return before delete-policy check)."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.discard_if(
            "k", condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# AppendOnlyDictCached
# ═══════════════════════════════════════════════════════════════════════


class TestAppendOnlyDictCachedAnyEtag:

    def _make(self, tmp_path) -> AppendOnlyDictCached:
        main = FileDirDict(
            base_dir=str(tmp_path / "main"), append_only=True,
            serialization_format="json")
        cache = FileDirDict(
            base_dir=str(tmp_path / "cache"), append_only=True,
            serialization_format="json")
        return AppendOnlyDictCached(main_dict=main, data_cache=cache)

    def test_get_item_if_item_not_available_expected_retrieves(
            self, tmp_path):
        """AppendOnlyDictCached get_item_if + ANY_ETAG + ITEM_NOT_AVAILABLE
        expected: value retrieved (expected != actual)."""
        d = self._make(tmp_path)
        d["k"] = "val"

        result = d.get_item_if(
            "k", condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_get_item_if_matching_expected_default_skips(self, tmp_path):
        """AppendOnlyDictCached get_item_if + matching expected + default
        retrieve: VALUE_NOT_RETRIEVED."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ANY_ETAG, expected_etag=etag)

        assert result.condition_was_satisfied
        assert result.new_value is VALUE_NOT_RETRIEVED

    def test_get_item_if_always_retrieve(self, tmp_path):
        """AppendOnlyDictCached get_item_if + ALWAYS_RETRIEVE: returns value."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ANY_ETAG, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_set_item_if_insert_when_absent(self, tmp_path):
        """AppendOnlyDictCached set_item_if + ANY_ETAG inserts when absent."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "val"

    def test_set_item_if_keep_current_existing_key(self, tmp_path):
        """AppendOnlyDictCached set_item_if KEEP_CURRENT on existing key."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ANY_ETAG, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "val"

    def test_setdefault_if_insert_when_absent(self, tmp_path):
        """AppendOnlyDictCached setdefault_if + ANY_ETAG inserts when absent."""
        d = self._make(tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ANY_ETAG,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_no_overwrite(self, tmp_path):
        """AppendOnlyDictCached setdefault_if + ANY_ETAG on existing key:
        no overwrite."""
        d = self._make(tmp_path)
        d["k"] = "existing"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ANY_ETAG, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "existing"

    def test_discard_if_raises_mutation_policy_error(self, tmp_path):
        """AppendOnlyDictCached discard_if always raises MutationPolicyError."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        with pytest.raises(MutationPolicyError):
            d.discard_if(
                "k", condition=ANY_ETAG, expected_etag=etag)
