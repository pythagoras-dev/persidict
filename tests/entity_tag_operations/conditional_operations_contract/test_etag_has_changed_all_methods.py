"""Comprehensive tests for ETAG_HAS_CHANGED across all conditional methods and classes.

Covers set_item_if, get_item_if, setdefault_if, discard_if, and transform_item
for every concrete PersiDict implementation. ETAG_HAS_CHANGED requires expected
and actual ETags to differ, so the condition fires when the key has been modified
since the caller last observed it. Key scenarios:
    - Real ETag vs different real ETag (satisfied)
    - Real ETag vs same real ETag (not satisfied)
    - Real ETag vs ITEM_NOT_AVAILABLE (satisfied: key disappeared)
    - ITEM_NOT_AVAILABLE vs real ETag (satisfied: key appeared)
    - ITEM_NOT_AVAILABLE vs ITEM_NOT_AVAILABLE (NOT satisfied: both absent)
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
    ETAG_HAS_CHANGED,
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
    IF_ETAG_CHANGED,
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
        bucket_name="etag-changed-all-methods", serialization_format="json")


def _build_s3_cached(tmp_path) -> S3Dict_FileDirCached:
    return S3Dict_FileDirCached(
        bucket_name="etag-changed-cached",
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
# set_item_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetItemIfEtagHasChanged:

    def test_mismatched_etag_allows_write(self, tmp_path, spec):
        """Stale expected ETag on existing key: condition satisfied, write goes through."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag_before = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag_before))

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag_before
            assert result.resulting_etag != etag_before
            assert result.resulting_etag == d.etag("k")
            assert result.new_value == "v2"
            assert d["k"] == "v2"

    def test_matching_etag_blocks_write(self, tmp_path, spec):
        """Matching ETag: condition not satisfied, no write occurs."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "v1"

    def test_matching_etag_default_retrieve_returns_value_not_retrieved(
            self, tmp_path, spec):
        """Matching ETag + default retrieve: expected == actual so
        IF_ETAG_CHANGED skips fetch, returns VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_matching_etag_always_retrieve_returns_current_value(
            self, tmp_path, spec):
        """Matching ETag + ALWAYS_RETRIEVE: returns the existing value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_HAS_CHANGED, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value == "original"

    def test_matching_etag_never_retrieve(self, tmp_path, spec):
        """Matching ETag + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_HAS_CHANGED, expected_etag=etag,
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "original"

    def test_mismatch_new_value_field_equals_written_value(
            self, tmp_path, spec):
        """On successful write, new_value should equal the value that was written."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "old"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert result.new_value == "new"

    def test_item_not_available_expected_on_existing_key_satisfied(
            self, tmp_path, spec):
        """ITEM_NOT_AVAILABLE expected on existing key: ETags differ, write proceeds."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == d.etag("k")
            assert d["k"] == "v2"

    def test_item_not_available_expected_on_missing_key_not_satisfied(
            self, tmp_path, spec):
        """ITEM_NOT_AVAILABLE expected on missing key: both are ITEM_NOT_AVAILABLE,
        condition NOT satisfied."""
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
            assert "k" not in d

    def test_real_etag_on_missing_key_satisfied(self, tmp_path, spec):
        """Real expected_etag on missing key: ETags differ (real vs absent),
        condition satisfied but key stays absent (no actual value to overwrite)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.set_item_if(
                "temp", value="new",
                condition=ETAG_HAS_CHANGED,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert d["temp"] == "new"

    def test_keep_current_mismatch_no_mutation(self, tmp_path, spec):
        """KEEP_CURRENT + mismatched ETag: satisfied but no mutation."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"

    def test_keep_current_mismatch_default_retrieve_returns_value(
            self, tmp_path, spec):
        """KEEP_CURRENT + mismatched ETag + IF_ETAG_CHANGED (default): value
        is retrieved because expected != actual."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"

    def test_keep_current_matching_etag_condition_fails(self, tmp_path, spec):
        """KEEP_CURRENT + matching ETag: condition not satisfied, no mutation."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert d["k"] == "preserved"

    def test_keep_current_item_not_available_on_existing_key(
            self, tmp_path, spec):
        """KEEP_CURRENT + ITEM_NOT_AVAILABLE expected on existing key: satisfied,
        no mutation, value returned."""
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

    def test_keep_current_item_not_available_on_missing_key_not_satisfied(
            self, tmp_path, spec):
        """KEEP_CURRENT + ITEM_NOT_AVAILABLE on missing key: both absent,
        condition NOT satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_mismatch_deletes_key(self, tmp_path, spec):
        """DELETE_CURRENT + mismatched ETag: condition satisfied, key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_matching_etag_no_delete(self, tmp_path, spec):
        """DELETE_CURRENT + matching ETag: condition not satisfied, key survives."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "survivor"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert d["k"] == "survivor"

    def test_delete_current_item_not_available_on_missing_key(
            self, tmp_path, spec):
        """DELETE_CURRENT + ITEM_NOT_AVAILABLE on missing key: not satisfied,
        both absent."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE

    def test_delete_current_item_not_available_on_existing_key(
            self, tmp_path, spec):
        """DELETE_CURRENT + ITEM_NOT_AVAILABLE on existing key: satisfied, deletes."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_two_successive_writes_second_with_stale_etag_fails(
            self, tmp_path, spec):
        """After a successful write, the old ETag now matches the stale one, so a
        second ETAG_HAS_CHANGED with the new ETag should fail (new == actual)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag1 = d.etag("k")

            r1 = d.set_item_if(
                "k", value="v2",
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag1))
            assert r1.condition_was_satisfied
            assert d["k"] == "v2"
            etag2 = d.etag("k")

            r2 = d.set_item_if(
                "k", value="v3",
                condition=ETAG_HAS_CHANGED, expected_etag=etag2)
            assert not r2.condition_was_satisfied
            assert d["k"] == "v2"


# ═══════════════════════════════════════════════════════════════════════
# get_item_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestGetItemIfEtagHasChanged:

    def test_mismatched_etag_satisfied_default_retrieve_returns_value(
            self, tmp_path, spec):
        """Mismatched ETag + IF_ETAG_CHANGED (default): condition satisfied,
        value retrieved (expected != actual)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_matching_etag_not_satisfied_default_retrieve_skips_value(
            self, tmp_path, spec):
        """Matching ETag: condition not satisfied, default retrieve skips value
        (expected == actual)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_matching_etag_always_retrieve_returns_value(
            self, tmp_path, spec):
        """Matching ETag + ALWAYS_RETRIEVE: condition fails but value returned."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_matching_etag_never_retrieve(self, tmp_path, spec):
        """Matching ETag + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED, expected_etag=etag,
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_mismatched_etag_always_retrieve(self, tmp_path, spec):
        """Mismatched ETag + ALWAYS_RETRIEVE: satisfied, value returned."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.new_value == "v1"

    def test_mismatched_etag_never_retrieve(self, tmp_path, spec):
        """Mismatched ETag + NEVER_RETRIEVE: satisfied, VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=NEVER_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_item_not_available_expected_on_existing_key_satisfied(
            self, tmp_path, spec):
        """ITEM_NOT_AVAILABLE expected on existing key: satisfied (absent != real)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_item_not_available_expected_on_missing_key_not_satisfied(
            self, tmp_path, spec):
        """ITEM_NOT_AVAILABLE expected on missing key: both absent,
        condition NOT satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_real_etag_on_missing_key_satisfied(self, tmp_path, spec):
        """Real expected_etag on missing key: satisfied (real != absent)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.get_item_if(
                "temp", condition=ETAG_HAS_CHANGED,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_no_mutation_on_dict(self, tmp_path, spec):
        """get_item_if with ETAG_HAS_CHANGED should never mutate the dict."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)
            d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)
            d.get_item_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert d["k"] == "v1"
            assert d.etag("k") == etag
            assert len(d) == 1


# ═══════════════════════════════════════════════════════════════════════
# setdefault_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetdefaultIfEtagHasChanged:

    def test_missing_key_item_not_available_not_satisfied(
            self, tmp_path, spec):
        """Absent key + ITEM_NOT_AVAILABLE expected: both absent, condition NOT
        satisfied. No insert."""
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

    def test_missing_key_real_etag_satisfied_inserts(self, tmp_path, spec):
        """Absent key + real expected ETag: satisfied (real != absent), insert."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.setdefault_if(
                "temp", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag == d.etag("temp")
            assert result.new_value == "default"
            assert d["temp"] == "default"

    def test_existing_key_matching_etag_not_satisfied(self, tmp_path, spec):
        """Existing key + matching ETag: condition not satisfied, no overwrite."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_matching_etag_default_retrieve_skips_value(
            self, tmp_path, spec):
        """Existing key + matching ETag + default retrieve: value not fetched
        (etags match so IF_ETAG_CHANGED skips)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_existing_key_matching_etag_always_retrieve_returns_value(
            self, tmp_path, spec):
        """Existing key + matching ETag + ALWAYS_RETRIEVE: returns value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value == "existing"

    def test_existing_key_mismatched_etag_satisfied_no_overwrite(
            self, tmp_path, spec):
        """Existing key + mismatched ETag: condition satisfied, but setdefault
        never overwrites existing keys."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_mismatched_default_retrieve_returns_value(
            self, tmp_path, spec):
        """Existing key + mismatched ETag + default retrieve: value is returned
        (etags differ so IF_ETAG_CHANGED fetches)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert result.new_value == "existing"

    def test_existing_key_item_not_available_expected_satisfied(
            self, tmp_path, spec):
        """Existing key + expected ITEM_NOT_AVAILABLE: satisfied (absent != real),
        but setdefault does not overwrite."""
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
            assert result.new_value == "existing"
            assert d["k"] == "existing"


# ═══════════════════════════════════════════════════════════════════════
# discard_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestDiscardIfEtagHasChanged:

    def test_mismatched_etag_deletes_key(self, tmp_path, spec):
        """Mismatched ETag: condition satisfied, key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_matching_etag_no_delete(self, tmp_path, spec):
        """Matching ETag: condition not satisfied, key survives."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "v1"

    def test_item_not_available_expected_on_missing_key_not_satisfied(
            self, tmp_path, spec):
        """Missing key + ITEM_NOT_AVAILABLE: both absent, condition NOT satisfied."""
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

    def test_real_etag_on_missing_key_satisfied_noop(self, tmp_path, spec):
        """Missing key + real ETag: satisfied (real != absent), but nothing to delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.discard_if(
                "temp", condition=ETAG_HAS_CHANGED,
                expected_etag=stale_etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_item_not_available_expected_on_existing_key_satisfied(
            self, tmp_path, spec):
        """Existing key + ITEM_NOT_AVAILABLE expected: satisfied (absent != real),
        key deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_then_retry_with_item_not_available(self, tmp_path, spec):
        """After deleting a key, retrying discard_if with ITEM_NOT_AVAILABLE
        should NOT be satisfied (both absent)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            r1 = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=mismatched_etag(spec, etag))
            assert r1.condition_was_satisfied
            assert "k" not in d

            r2 = d.discard_if(
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)
            assert not r2.condition_was_satisfied


# ═══════════════════════════════════════════════════════════════════════
# EmptyDict
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyDictEtagHasChanged:

    def test_get_item_if_item_not_available_not_satisfied(self):
        """EmptyDict get_item_if + ITEM_NOT_AVAILABLE: both absent, not satisfied."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_real_etag_satisfied(self):
        """EmptyDict get_item_if + real ETag: satisfied (real != absent)."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_item_not_available_not_satisfied(self):
        """EmptyDict set_item_if + ITEM_NOT_AVAILABLE: both absent, not satisfied."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_set_item_if_real_etag_satisfied_silently_discards(self):
        """EmptyDict set_item_if + real ETag: satisfied but write discarded."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_setdefault_if_item_not_available_not_satisfied(self):
        """EmptyDict setdefault_if + ITEM_NOT_AVAILABLE: both absent, not satisfied."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_setdefault_if_real_etag_satisfied_silently_discards(self):
        """EmptyDict setdefault_if + real ETag: satisfied but insert discarded."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_discard_if_item_not_available_not_satisfied(self):
        """EmptyDict discard_if + ITEM_NOT_AVAILABLE: both absent, not satisfied."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_discard_if_real_etag_satisfied(self):
        """EmptyDict discard_if + real ETag: satisfied (real != absent), no-op."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag="some_etag")

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_keep_current_not_satisfied(self):
        """EmptyDict set_item_if KEEP_CURRENT + ITEM_NOT_AVAILABLE:
        both absent, not satisfied."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_set_item_if_delete_current_not_satisfied(self):
        """EmptyDict set_item_if DELETE_CURRENT + ITEM_NOT_AVAILABLE:
        both absent, not satisfied."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# WriteOnceDict
# ═══════════════════════════════════════════════════════════════════════


class TestWriteOnceDictEtagHasChanged:

    def test_set_item_if_raises_mutation_policy_error(self, tmp_path):
        """WriteOnceDict.set_item_if always raises MutationPolicyError."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value="val",
                condition=ETAG_HAS_CHANGED,
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
                condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_get_item_if_real_etag_satisfied(self, tmp_path):
        """WriteOnceDict.get_item_if + ETAG_HAS_CHANGED + real ETag on existing
        key with mismatched expected: satisfied, returns value."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_get_item_if_matching_etag_not_satisfied(self, tmp_path):
        """WriteOnceDict.get_item_if + matching ETag: not satisfied."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

        assert not result.condition_was_satisfied

    def test_setdefault_if_existing_key_mismatched_etag(self, tmp_path):
        """WriteOnceDict.setdefault_if on existing key + mismatched ETag:
        satisfied, no overwrite."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "val"
        assert d["k"] == "val"

    def test_setdefault_if_absent_key_real_etag_inserts(self, tmp_path):
        """WriteOnceDict.setdefault_if + real expected ETag on absent key:
        satisfied, inserts default."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag="some_stale_etag")

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_discard_if_missing_key_item_not_available_not_satisfied(
            self, tmp_path):
        """WriteOnceDict.discard_if + ITEM_NOT_AVAILABLE on missing key:
        not satisfied."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# AppendOnlyDictCached
# ═══════════════════════════════════════════════════════════════════════


class TestAppendOnlyDictCachedEtagHasChanged:

    def _make(self, tmp_path) -> AppendOnlyDictCached:
        main = FileDirDict(
            base_dir=str(tmp_path / "main"), append_only=True,
            serialization_format="json")
        cache = FileDirDict(
            base_dir=str(tmp_path / "cache"), append_only=True,
            serialization_format="json")
        return AppendOnlyDictCached(main_dict=main, data_cache=cache)

    def test_get_item_if_mismatched_etag_returns_value(self, tmp_path):
        """AppendOnlyDictCached get_item_if + mismatched ETag: satisfied."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_get_item_if_matching_etag_not_satisfied(self, tmp_path):
        """AppendOnlyDictCached get_item_if + matching ETag: not satisfied."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

        assert not result.condition_was_satisfied
        assert result.new_value is VALUE_NOT_RETRIEVED

    def test_get_item_if_matching_etag_always_retrieve_returns_value(
            self, tmp_path):
        """AppendOnlyDictCached get_item_if + matching ETag + ALWAYS_RETRIEVE."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert not result.condition_was_satisfied
        assert result.new_value == "val"

    def test_set_item_if_insert_with_real_etag_on_absent_key(self, tmp_path):
        """AppendOnlyDictCached set_item_if + real ETag on absent key:
        satisfied, inserts."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag="some_stale_etag")

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "val"

    def test_set_item_if_item_not_available_on_absent_not_satisfied(
            self, tmp_path):
        """AppendOnlyDictCached set_item_if + ITEM_NOT_AVAILABLE on absent key:
        not satisfied."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert not result.value_was_mutated
        assert "k" not in d

    def test_set_item_if_keep_current_mismatched_etag(self, tmp_path):
        """AppendOnlyDictCached set_item_if KEEP_CURRENT + mismatched ETag."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "val"

    def test_setdefault_if_absent_key_real_etag_inserts(self, tmp_path):
        """AppendOnlyDictCached setdefault_if + real ETag on absent key:
        satisfied, inserts."""
        d = self._make(tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag="some_stale_etag")

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_mismatched_no_overwrite(
            self, tmp_path):
        """AppendOnlyDictCached setdefault_if + mismatched ETag on existing key:
        satisfied, no overwrite."""
        d = self._make(tmp_path)
        d["k"] = "existing"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
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
                "k", condition=ETAG_HAS_CHANGED,
                expected_etag=ITEM_NOT_AVAILABLE)


# ═══════════════════════════════════════════════════════════════════════
# MutableDictCached (S3-backed main with caches)
# ═══════════════════════════════════════════════════════════════════════


class TestMutableDictCachedEtagHasChanged:

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
    def test_set_item_if_mismatch_writes_and_updates_caches(self, tmp_path):
        """MutableDictCached set_item_if + mismatched ETag: write succeeds."""
        d = self._make("mc-hc-set-mismatch", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value="v2",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "v2"

    @mock_aws
    def test_set_item_if_match_blocks_write(self, tmp_path):
        """MutableDictCached set_item_if + matching ETag: no write."""
        d = self._make("mc-hc-set-match", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value="v2",
            condition=ETAG_HAS_CHANGED, expected_etag=etag)

        assert not result.condition_was_satisfied
        assert d["k"] == "v1"

    @mock_aws
    def test_discard_if_mismatch_removes(self, tmp_path):
        """MutableDictCached discard_if + mismatched ETag: deletes."""
        d = self._make("mc-hc-discard", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert "k" not in d

    @mock_aws
    def test_discard_if_match_preserves(self, tmp_path):
        """MutableDictCached discard_if + matching ETag: key survives."""
        d = self._make("mc-hc-discard-match", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.discard_if(
            "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

        assert not result.condition_was_satisfied
        assert d["k"] == "v1"

    @mock_aws
    def test_get_item_if_mismatch_returns_value(self, tmp_path):
        """MutableDictCached get_item_if + mismatched ETag: returns value."""
        d = self._make("mc-hc-get", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "v1"

    @mock_aws
    def test_get_item_if_match_returns_value_not_retrieved(self, tmp_path):
        """MutableDictCached get_item_if + matching ETag: VALUE_NOT_RETRIEVED."""
        d = self._make("mc-hc-get-match", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_HAS_CHANGED, expected_etag=etag)

        assert not result.condition_was_satisfied
        assert result.new_value is VALUE_NOT_RETRIEVED

    @mock_aws
    def test_setdefault_if_absent_key_real_etag_inserts(self, tmp_path):
        """MutableDictCached setdefault_if + real ETag on absent key: inserts."""
        d = self._make("mc-hc-setdef", tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag="stale_etag")

        assert result.condition_was_satisfied
        assert d["k"] == "default"

    @mock_aws
    def test_setdefault_if_absent_key_item_not_available_not_satisfied(
            self, tmp_path):
        """MutableDictCached setdefault_if + ITEM_NOT_AVAILABLE on absent key:
        not satisfied."""
        d = self._make("mc-hc-setdef-na", tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert not result.condition_was_satisfied
        assert "k" not in d

    @mock_aws
    def test_set_item_if_keep_current_mismatch(self, tmp_path):
        """MutableDictCached set_item_if KEEP_CURRENT + mismatched ETag:
        satisfied, no mutation."""
        d = self._make("mc-hc-keep", tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert d["k"] == "val"
        assert result.new_value == "val"

    @mock_aws
    def test_set_item_if_delete_current_mismatch(self, tmp_path):
        """MutableDictCached set_item_if DELETE_CURRENT + mismatched ETag:
        key removed."""
        d = self._make("mc-hc-del", tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert "k" not in d
