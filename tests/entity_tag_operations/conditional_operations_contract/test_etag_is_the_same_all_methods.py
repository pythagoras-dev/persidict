"""Comprehensive tests for ETAG_IS_THE_SAME across all conditional methods and classes.

Covers set_item_if, get_item_if, setdefault_if, discard_if, and transform_item
for every concrete PersiDict implementation. Exercises matched ETags, mismatched
ETags, missing-key scenarios, retrieve_value interactions, joker values
(KEEP_CURRENT, DELETE_CURRENT), and result-field correctness.
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
    return FileDirDict(base_dir=str(tmp_path / "file"), serialization_format="json")


def _build_basic_s3(_: object) -> BasicS3Dict:
    return BasicS3Dict(
        bucket_name="etag-same-all-methods", serialization_format="json")


def _build_s3_cached(tmp_path) -> S3Dict_FileDirCached:
    return S3Dict_FileDirCached(
        bucket_name="etag-same-cached",
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
class TestSetItemIfEtagIsTheSame:

    def test_match_writes_new_value(self, tmp_path, spec):
        """Matching ETag should allow the write and return the new value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag_before = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ETAG_IS_THE_SAME, expected_etag=etag_before)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag_before
            assert result.resulting_etag != etag_before
            assert result.resulting_etag == d.etag("k")
            assert d["k"] == "v2"

    def test_match_new_value_field_equals_written_value(self, tmp_path, spec):
        """On successful write, new_value should be the value that was written."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "old"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert result.new_value == "new"

    def test_mismatch_no_write(self, tmp_path, spec):
        """Mismatched ETag should block the write."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="v2",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "v1"

    def test_mismatch_returns_current_value_default_retrieve(
            self, tmp_path, spec):
        """Mismatch with default retrieve_value should return current value
        (actual_etag != expected_etag triggers retrieval)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert result.new_value == "original"

    def test_mismatch_never_retrieve_returns_value_not_retrieved(
            self, tmp_path, spec):
        """Mismatch + NEVER_RETRIEVE should return VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "original"

    def test_mismatch_always_retrieve_returns_current_value(
            self, tmp_path, spec):
        """Mismatch + ALWAYS_RETRIEVE should return the current value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "original"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value="replacement",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value == "original"

    def test_insert_when_missing_with_item_not_available(
            self, tmp_path, spec):
        """ETAG_IS_THE_SAME + ITEM_NOT_AVAILABLE inserts into empty dict."""
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

    def test_fails_on_existing_key_with_item_not_available(
            self, tmp_path, spec):
        """ETAG_IS_THE_SAME + ITEM_NOT_AVAILABLE on existing key should fail."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"

            result = d.set_item_if(
                "k", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert d["k"] == "existing"

    def test_fails_on_missing_key_with_real_etag(self, tmp_path, spec):
        """ETAG_IS_THE_SAME + real ETag on missing key should fail."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.set_item_if(
                "temp", value="new",
                condition=ETAG_IS_THE_SAME,
                expected_etag=stale_etag)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "temp" not in d

    def test_keep_current_match_no_mutation(self, tmp_path, spec):
        """KEEP_CURRENT + matching ETag: no mutation, etag unchanged."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_IS_THE_SAME, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"
            assert d.etag("k") == etag

    def test_keep_current_match_default_retrieve_returns_value_not_retrieved(
            self, tmp_path, spec):
        """KEEP_CURRENT + matching ETag + default retrieve: etags match so
        IF_ETAG_CHANGED skips retrieval."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_keep_current_mismatch_no_mutation(self, tmp_path, spec):
        """KEEP_CURRENT + mismatched ETag: condition fails, no mutation."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "preserved"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.new_value == "preserved"
            assert d["k"] == "preserved"

    def test_keep_current_missing_key_item_not_available(
            self, tmp_path, spec):
        """KEEP_CURRENT + ITEM_NOT_AVAILABLE on missing key: satisfied,
        key stays absent."""
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

    def test_delete_current_match_deletes_key(self, tmp_path, spec):
        """DELETE_CURRENT + matching ETag: key should be deleted."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_delete_current_mismatch_no_delete(self, tmp_path, spec):
        """DELETE_CURRENT + mismatched ETag: key should survive."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "survivor"
            etag = d.etag("k")

            result = d.set_item_if(
                "k", value=DELETE_CURRENT,
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert d["k"] == "survivor"

    def test_delete_current_missing_key(self, tmp_path, spec):
        """DELETE_CURRENT + ITEM_NOT_AVAILABLE on missing key: no-op."""
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

    def test_two_successive_conditional_writes(self, tmp_path, spec):
        """Second write with stale ETag should fail."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag1 = d.etag("k")

            r1 = d.set_item_if(
                "k", value="v2",
                condition=ETAG_IS_THE_SAME, expected_etag=etag1)
            assert r1.condition_was_satisfied
            assert d["k"] == "v2"

            r2 = d.set_item_if(
                "k", value="v3",
                condition=ETAG_IS_THE_SAME, expected_etag=etag1)
            assert not r2.condition_was_satisfied
            assert d["k"] == "v2"


# ═══════════════════════════════════════════════════════════════════════
# get_item_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestGetItemIfEtagIsTheSame:

    def test_match_default_retrieve_skips_value(self, tmp_path, spec):
        """Matching ETag + IF_ETAG_CHANGED (default): value not retrieved."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_match_always_retrieve_returns_value(self, tmp_path, spec):
        """Matching ETag + ALWAYS_RETRIEVE should return the value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_match_never_retrieve_returns_value_not_retrieved(
            self, tmp_path, spec):
        """Matching ETag + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
                retrieve_value=NEVER_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_mismatch_returns_value(self, tmp_path, spec):
        """Mismatched ETag should return the current value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert result.actual_etag == etag
            assert result.new_value == "v1"

    def test_mismatch_never_retrieve_returns_value_not_retrieved(
            self, tmp_path, spec):
        """Mismatched ETag + NEVER_RETRIEVE: VALUE_NOT_RETRIEVED."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=NEVER_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_mismatch_always_retrieve_returns_value(self, tmp_path, spec):
        """Mismatched ETag + ALWAYS_RETRIEVE: returns current value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag),
                retrieve_value=ALWAYS_RETRIEVE)

            assert not result.condition_was_satisfied
            assert result.new_value == "v1"

    def test_missing_key_item_not_available_satisfied(self, tmp_path, spec):
        """Missing key + ITEM_NOT_AVAILABLE: condition satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_missing_key_real_etag_not_satisfied(self, tmp_path, spec):
        """Missing key + real ETag: condition not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.get_item_if(
                "temp", condition=ETAG_IS_THE_SAME,
                expected_etag=stale_etag)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_existing_key_item_not_available_not_satisfied(
            self, tmp_path, spec):
        """Existing key + ITEM_NOT_AVAILABLE expected: not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert result.actual_etag == etag

    def test_no_mutation_on_dict(self, tmp_path, spec):
        """get_item_if should never mutate the dict regardless of result."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
            d.get_item_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert d["k"] == "v1"
            assert d.etag("k") == etag
            assert len(d) == 1


# ═══════════════════════════════════════════════════════════════════════
# setdefault_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestSetdefaultIfEtagIsTheSame:

    def test_missing_key_item_not_available_inserts(self, tmp_path, spec):
        """Absent key + ITEM_NOT_AVAILABLE: should insert default_value."""
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

    def test_missing_key_real_etag_no_insert(self, tmp_path, spec):
        """Absent key + real ETag: condition not satisfied, no insert."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.setdefault_if(
                "temp", default_value="default",
                condition=ETAG_IS_THE_SAME,
                expected_etag=stale_etag)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "temp" not in d

    def test_existing_key_match_no_overwrite(self, tmp_path, spec):
        """Existing key + matching ETag: satisfied but no overwrite."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert d["k"] == "existing"

    def test_existing_key_match_default_retrieve_skips_value(
            self, tmp_path, spec):
        """Existing key + matching ETag + default retrieve: value not fetched
        (etags match so IF_ETAG_CHANGED skips)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert result.new_value is VALUE_NOT_RETRIEVED

    def test_existing_key_match_always_retrieve_returns_value(
            self, tmp_path, spec):
        """Existing key + matching ETag + ALWAYS_RETRIEVE: returns value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME, expected_etag=etag,
                retrieve_value=ALWAYS_RETRIEVE)

            assert result.condition_was_satisfied
            assert result.new_value == "existing"

    def test_existing_key_mismatch_no_overwrite(self, tmp_path, spec):
        """Existing key + mismatched ETag: not satisfied, no overwrite."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert d["k"] == "existing"

    def test_existing_key_mismatch_default_retrieve_returns_value(
            self, tmp_path, spec):
        """Existing key + mismatched ETag + default retrieve: should return
        the existing value (etags differ so IF_ETAG_CHANGED fetches)."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "existing"
            etag = d.etag("k")

            result = d.setdefault_if(
                "k", default_value="default",
                condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert result.new_value == "existing"

    def test_existing_key_item_not_available_expected_not_satisfied(
            self, tmp_path, spec):
        """Existing key + expected ITEM_NOT_AVAILABLE: not satisfied."""
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


# ═══════════════════════════════════════════════════════════════════════
# discard_if
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestDiscardIfEtagIsTheSame:

    def test_match_deletes_key(self, tmp_path, spec):
        """Matching ETag should delete the key."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

            assert result.condition_was_satisfied
            assert result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_mismatch_no_delete(self, tmp_path, spec):
        """Mismatched ETag should not delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=mismatched_etag(spec, etag))

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert result.resulting_etag == etag
            assert result.new_value is VALUE_NOT_RETRIEVED
            assert d["k"] == "v1"

    def test_missing_key_item_not_available_satisfied(self, tmp_path, spec):
        """Missing key + ITEM_NOT_AVAILABLE: satisfied, no-op."""
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

    def test_missing_key_real_etag_not_satisfied(self, tmp_path, spec):
        """Missing key + real ETag: not satisfied."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["temp"] = "x"
            stale_etag = d.etag("temp")
            del d["temp"]

            result = d.discard_if(
                "temp", condition=ETAG_IS_THE_SAME,
                expected_etag=stale_etag)

            assert not result.condition_was_satisfied
            assert result.actual_etag is ITEM_NOT_AVAILABLE
            assert result.new_value is ITEM_NOT_AVAILABLE

    def test_existing_key_item_not_available_not_satisfied(
            self, tmp_path, spec):
        """Existing key + ITEM_NOT_AVAILABLE: not satisfied, no delete."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            result = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

            assert not result.condition_was_satisfied
            assert not result.value_was_mutated
            assert result.actual_etag == etag
            assert d["k"] == "v1"

    def test_delete_then_retry_with_stale_etag(self, tmp_path, spec):
        """After successful delete, using old ETag should fail."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "v1"
            etag = d.etag("k")

            r1 = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
            assert r1.condition_was_satisfied
            assert "k" not in d

            r2 = d.discard_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
            assert not r2.condition_was_satisfied


# ═══════════════════════════════════════════════════════════════════════
# transform_item  (uses ETAG_IS_THE_SAME internally)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
class TestTransformItemEtagIsTheSame:

    def test_basic_transform_updates_value(self, tmp_path, spec):
        """transform_item should write the transformed value."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = 10

            result = d.transform_item(
                "k", transformer=lambda v: v + 5)

            assert result.new_value == 15
            assert d["k"] == 15

    def test_transform_missing_key_creates_it(self, tmp_path, spec):
        """transform_item on absent key receives ITEM_NOT_AVAILABLE."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)

            result = d.transform_item(
                "k", transformer=lambda v: "created"
                if v is ITEM_NOT_AVAILABLE else "wrong")

            assert result.new_value == "created"
            assert d["k"] == "created"

    def test_transform_delete_current(self, tmp_path, spec):
        """transform_item returning DELETE_CURRENT should remove the key."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "doomed"

            result = d.transform_item(
                "k", transformer=lambda v: DELETE_CURRENT)

            assert result.new_value is ITEM_NOT_AVAILABLE
            assert result.resulting_etag is ITEM_NOT_AVAILABLE
            assert "k" not in d

    def test_transform_keep_current_noop(self, tmp_path, spec):
        """transform_item returning KEEP_CURRENT: no mutation."""
        with maybe_mock_aws(spec["uses_s3"]):
            d = spec["factory"](tmp_path)
            d["k"] = "stable"
            etag = d.etag("k")

            result = d.transform_item(
                "k", transformer=lambda v: KEEP_CURRENT)

            assert result.new_value == "stable"
            assert result.resulting_etag == etag
            assert d["k"] == "stable"


# ═══════════════════════════════════════════════════════════════════════
# EmptyDict
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyDictEtagIsTheSame:

    def test_get_item_if_item_not_available_satisfied(self):
        """EmptyDict get_item_if + ITEM_NOT_AVAILABLE: satisfied, absent."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_get_item_if_real_etag_not_satisfied(self):
        """EmptyDict get_item_if + real ETag: not satisfied."""
        d = EmptyDict()
        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag="some_etag")

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_set_item_if_item_not_available_silently_discards(self):
        """EmptyDict set_item_if + ITEM_NOT_AVAILABLE: satisfied but
        write is silently discarded."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_set_item_if_real_etag_not_satisfied(self):
        """EmptyDict set_item_if + real ETag: not satisfied."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag="some_etag")

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_setdefault_if_item_not_available_silently_discards(self):
        """EmptyDict setdefault_if + ITEM_NOT_AVAILABLE: satisfied,
        insert discarded."""
        d = EmptyDict()
        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.resulting_etag is ITEM_NOT_AVAILABLE
        assert "k" not in d

    def test_discard_if_item_not_available_satisfied(self):
        """EmptyDict discard_if + ITEM_NOT_AVAILABLE: satisfied, no-op."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_discard_if_real_etag_not_satisfied(self):
        """EmptyDict discard_if + real ETag: not satisfied."""
        d = EmptyDict()
        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME,
            expected_etag="some_etag")

        assert not result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE

    def test_set_item_if_keep_current_item_not_available(self):
        """EmptyDict set_item_if KEEP_CURRENT + ITEM_NOT_AVAILABLE:
        satisfied, key stays absent."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value is ITEM_NOT_AVAILABLE

    def test_set_item_if_delete_current_item_not_available(self):
        """EmptyDict set_item_if DELETE_CURRENT + ITEM_NOT_AVAILABLE:
        satisfied, nothing to delete."""
        d = EmptyDict()
        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.actual_etag is ITEM_NOT_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════
# WriteOnceDict
# ═══════════════════════════════════════════════════════════════════════


class TestWriteOnceDictEtagIsTheSame:

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

    def test_set_item_if_keep_current_also_raises(self, tmp_path):
        """WriteOnceDict.set_item_if raises even with KEEP_CURRENT."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)

        with pytest.raises(MutationPolicyError):
            d.set_item_if(
                "k", value=KEEP_CURRENT,
                condition=ETAG_IS_THE_SAME,
                expected_etag=ITEM_NOT_AVAILABLE)

    def test_get_item_if_delegates_to_wrapped(self, tmp_path):
        """WriteOnceDict.get_item_if should delegate and work correctly."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_setdefault_if_existing_key_match(self, tmp_path):
        """WriteOnceDict.setdefault_if on existing key + matching ETag."""
        inner = FileDirDict(
            base_dir=str(tmp_path), append_only=True,
            serialization_format="json")
        d = WriteOnceDict(wrapped_dict=inner)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"
        assert d["k"] == "val"

    def test_setdefault_if_inserts_when_absent(self, tmp_path):
        """WriteOnceDict.setdefault_if inserts when key is absent."""
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


# ═══════════════════════════════════════════════════════════════════════
# AppendOnlyDictCached
# ═══════════════════════════════════════════════════════════════════════


class TestAppendOnlyDictCachedEtagIsTheSame:

    def _make(self, tmp_path) -> AppendOnlyDictCached:
        main = FileDirDict(
            base_dir=str(tmp_path / "main"), append_only=True,
            serialization_format="json")
        cache = FileDirDict(
            base_dir=str(tmp_path / "cache"), append_only=True,
            serialization_format="json")
        return AppendOnlyDictCached(main_dict=main, data_cache=cache)

    def test_get_item_if_match_skips_value(self, tmp_path):
        """AppendOnlyDictCached.get_item_if + matching ETag."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

        assert result.condition_was_satisfied
        assert result.new_value is VALUE_NOT_RETRIEVED

    def test_get_item_if_match_always_retrieve(self, tmp_path):
        """AppendOnlyDictCached.get_item_if + matching ETag + ALWAYS_RETRIEVE."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "val"

    def test_set_item_if_insert_when_absent(self, tmp_path):
        """AppendOnlyDictCached.set_item_if inserts when key absent."""
        d = self._make(tmp_path)

        result = d.set_item_if(
            "k", value="val",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "val"

    def test_set_item_if_keep_current_existing_key(self, tmp_path):
        """AppendOnlyDictCached.set_item_if KEEP_CURRENT on existing key."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "val"

    def test_setdefault_if_insert_when_absent(self, tmp_path):
        """AppendOnlyDictCached.setdefault_if inserts when absent."""
        d = self._make(tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "default"

    def test_setdefault_if_existing_key_match(self, tmp_path):
        """AppendOnlyDictCached.setdefault_if on existing key + matching ETag."""
        d = self._make(tmp_path)
        d["k"] = "existing"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert result.new_value == "existing"

    def test_discard_if_raises_mutation_policy_error(self, tmp_path):
        """AppendOnlyDictCached.discard_if raises MutationPolicyError."""
        d = self._make(tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        with pytest.raises(MutationPolicyError):
            d.discard_if(
                "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

    def test_transform_item_raises_mutation_policy_error(self, tmp_path):
        """AppendOnlyDictCached.transform_item raises MutationPolicyError."""
        d = self._make(tmp_path)
        d["k"] = "val"

        with pytest.raises(MutationPolicyError):
            d.transform_item("k", transformer=lambda v: v)


# ═══════════════════════════════════════════════════════════════════════
# MutableDictCached  (S3-backed main with caches)
# ═══════════════════════════════════════════════════════════════════════


class TestMutableDictCachedEtagIsTheSame:

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
    def test_set_item_if_match_writes_and_updates_caches(self, tmp_path):
        """MutableDictCached set_item_if + matching ETag: write succeeds,
        subsequent read returns new value from cache."""
        d = self._make("mc-set-match", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value="v2",
            condition=ETAG_IS_THE_SAME, expected_etag=etag)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert d["k"] == "v2"

    @mock_aws
    def test_set_item_if_mismatch_preserves_caches(self, tmp_path):
        """MutableDictCached set_item_if mismatch: value unchanged."""
        d = self._make("mc-set-mismatch", tmp_path)
        d["k"] = "v1"

        result = d.set_item_if(
            "k", value="v2",
            condition=ETAG_IS_THE_SAME, expected_etag="wrong_etag")

        assert not result.condition_was_satisfied
        assert d["k"] == "v1"

    @mock_aws
    def test_discard_if_match_removes_from_caches(self, tmp_path):
        """MutableDictCached discard_if + matching ETag: key gone,
        caches purged."""
        d = self._make("mc-discard", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

        assert result.condition_was_satisfied
        assert "k" not in d

    @mock_aws
    def test_get_item_if_match_caches_result(self, tmp_path):
        """MutableDictCached get_item_if + ALWAYS_RETRIEVE: caches value."""
        d = self._make("mc-get", tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert result.new_value == "v1"
        assert d["k"] == "v1"

    @mock_aws
    def test_transform_item_updates_caches(self, tmp_path):
        """MutableDictCached transform_item: write + cache update."""
        d = self._make("mc-transform", tmp_path)
        d["k"] = 10

        result = d.transform_item("k", transformer=lambda v: v * 2)

        assert result.new_value == 20
        assert d["k"] == 20

    @mock_aws
    def test_setdefault_if_insert_updates_caches(self, tmp_path):
        """MutableDictCached setdefault_if insert: caches populated."""
        d = self._make("mc-setdef", tmp_path)

        result = d.setdefault_if(
            "k", default_value="default",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE)

        assert result.condition_was_satisfied
        assert d["k"] == "default"

    @mock_aws
    def test_set_item_if_keep_current_preserves_caches(self, tmp_path):
        """MutableDictCached set_item_if KEEP_CURRENT: caches unchanged."""
        d = self._make("mc-keep", tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=KEEP_CURRENT,
            condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)

        assert result.condition_was_satisfied
        assert not result.value_was_mutated
        assert d["k"] == "val"
        assert d.etag("k") == etag

    @mock_aws
    def test_set_item_if_delete_current_match(self, tmp_path):
        """MutableDictCached set_item_if DELETE_CURRENT: key removed."""
        d = self._make("mc-del", tmp_path)
        d["k"] = "val"
        etag = d.etag("k")

        result = d.set_item_if(
            "k", value=DELETE_CURRENT,
            condition=ETAG_IS_THE_SAME, expected_etag=etag)

        assert result.condition_was_satisfied
        assert result.value_was_mutated
        assert "k" not in d
