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
from persidict.cached_mutable_dict import MutableDictCached


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
    return BasicS3Dict(bucket_name="etag-is-the-same-basic", serialization_format="json")


def _build_s3_cached(tmp_path) -> S3Dict_FileDirCached:
    return S3Dict_FileDirCached(
        bucket_name="etag-is-the-same-cached",
        base_dir=str(tmp_path / "s3-cache"),
        serialization_format="json",
    )


def _build_mutable_cached(_: object) -> MutableDictCached:
    main = LocalDict(serialization_format="json")
    data_cache = LocalDict(serialization_format="pkl")
    etag_cache = LocalDict(serialization_format="json")
    return MutableDictCached(main_dict=main, data_cache=data_cache, etag_cache=etag_cache)


STANDARD_SPECS = [
    dict(name="local", uses_s3=False, factory=_build_local),
    dict(name="file", uses_s3=False, factory=_build_file),
    dict(name="basic_s3", uses_s3=True, factory=_build_basic_s3),
    dict(name="s3_cached", uses_s3=True, factory=_build_s3_cached),
    dict(name="mutable_cached", uses_s3=False, factory=_build_mutable_cached),
]


def mismatched_etag(spec: dict, etag: str) -> str:
    if spec["uses_s3"]:
        base = str(etag).strip('"')
        return f'"{base}-mismatch"'
    return f"{etag}-mismatch"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_get_item_if_etag_is_the_same_match_skips_value(tmp_path, spec):
    """ETAG_IS_THE_SAME with a matching ETag should skip value retrieval."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)

        assert result.condition_was_satisfied
        assert result.actual_etag == etag
        assert result.resulting_etag == etag
        assert result.new_value is VALUE_NOT_RETRIEVED
        assert d["k"] == "v1"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_get_item_if_etag_is_the_same_mismatch_returns_value(tmp_path, spec):
    """ETAG_IS_THE_SAME with a mismatched ETag should return the current value."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=mismatched_etag(spec, etag)
        )

        assert not result.condition_was_satisfied
        assert result.actual_etag == etag
        assert result.resulting_etag == etag
        assert result.new_value == "v1"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_set_item_if_etag_is_the_same_mismatch_no_mutation(tmp_path, spec):
    """Failed ETAG_IS_THE_SAME write should not mutate and should return current value."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.set_item_if(
            "k",
            value="v2",
            condition=ETAG_IS_THE_SAME,
            expected_etag=mismatched_etag(spec, etag),
        )

        assert not result.condition_was_satisfied
        assert result.actual_etag == etag
        assert result.resulting_etag == etag
        assert result.new_value == "v1"
        assert d["k"] == "v1"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_setdefault_if_etag_is_the_same_existing_skips_value(tmp_path, spec):
    """setdefault_if should skip retrieval when ETAG_IS_THE_SAME matches."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.setdefault_if(
            "k",
            default_value="new",
            condition=ETAG_IS_THE_SAME,
            expected_etag=etag,
        )

        assert result.condition_was_satisfied
        assert result.resulting_etag == etag
        assert result.new_value is VALUE_NOT_RETRIEVED
        assert d["k"] == "v1"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_discard_if_etag_is_the_same_mismatch_no_delete(tmp_path, spec):
    """ETAG_IS_THE_SAME mismatch should not delete and should not fetch value."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.discard_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=mismatched_etag(spec, etag)
        )

        assert not result.condition_was_satisfied
        assert result.actual_etag == etag
        assert result.resulting_etag == etag
        assert result.new_value is VALUE_NOT_RETRIEVED
        assert d["k"] == "v1"


@pytest.mark.parametrize("spec", STANDARD_SPECS, ids=[s["name"] for s in STANDARD_SPECS])
def test_set_item_if_etag_is_the_same_inserts_when_missing(tmp_path, spec):
    """ETAG_IS_THE_SAME with ITEM_NOT_AVAILABLE should insert when missing."""
    with maybe_mock_aws(spec["uses_s3"]):
        d = spec["factory"](tmp_path)

        result = d.set_item_if(
            "k",
            value="v1",
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE,
        )

        assert result.condition_was_satisfied
        assert result.actual_etag is ITEM_NOT_AVAILABLE
        assert d["k"] == "v1"
        assert result.resulting_etag == d.etag("k")
        assert result.new_value == "v1"
