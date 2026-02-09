from __future__ import annotations

from contextlib import contextmanager

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import (
    DELETE_CURRENT,
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT,
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
    ConditionalOperationResult,
)


MUTABLE_SPECS = [
    {
        "name": "local",
        "cls": LocalDict,
        "kwargs": {"serialization_format": "json"},
        "needs_base_dir": False,
        "uses_s3": False,
        "unknown_etag_means_changed": False,
    },
    {
        "name": "file",
        "cls": FileDirDict,
        "kwargs": {"serialization_format": "json"},
        "needs_base_dir": True,
        "uses_s3": False,
        "unknown_etag_means_changed": False,
    },
    {
        "name": "basic_s3",
        "cls": BasicS3Dict,
        "kwargs": {"serialization_format": "json", "bucket_name": "etag-basic"},
        "needs_base_dir": False,
        "uses_s3": True,
        "unknown_etag_means_changed": True,
    },
    {
        "name": "s3_cached",
        "cls": S3Dict_FileDirCached,
        "kwargs": {"serialization_format": "json", "bucket_name": "etag-cached"},
        "needs_base_dir": True,
        "uses_s3": True,
        "unknown_etag_means_changed": True,
    },
]


@contextmanager
def maybe_mock_aws(enabled: bool):
    if enabled:
        with mock_aws():
            yield
    else:
        yield


def build_dict(spec: dict, tmp_path):
    params = dict(spec["kwargs"])
    if spec["needs_base_dir"]:
        params["base_dir"] = str(tmp_path / spec["name"])
    return spec["cls"](**params)


def mismatched_etag(spec: dict, current_etag: str) -> str:
    if spec["uses_s3"]:
        base = str(current_etag).strip('"')
        return f"\"{base}-mismatch\""
    return "bogus"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_get_item_if_etag_different_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert not d.get_item_if("k", etag, ETAG_HAS_CHANGED).condition_was_satisfied

        result = d.get_item_if("k", mismatched_etag(spec, etag), ETAG_HAS_CHANGED)
        assert result.condition_was_satisfied
        assert result.new_value == "v1"
        assert result.resulting_etag == d.etag("k")


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_get_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        result = d.get_item_if("k", etag, ETAG_IS_THE_SAME)
        assert result.condition_was_satisfied
        assert result.new_value == "v1"
        assert result.resulting_etag == d.etag("k")

        assert not d.get_item_if("k", mismatched_etag(spec, etag), ETAG_IS_THE_SAME).condition_was_satisfied


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_equal_updates_and_rejects_mismatch(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res = d.set_item_if("k", "v2", etag, ETAG_IS_THE_SAME)
        assert res.condition_was_satisfied
        assert d["k"] == "v2"
        assert res.resulting_etag == d.etag("k")

        res_mismatch = d.set_item_if("k", "v3", mismatched_etag(spec, etag), ETAG_IS_THE_SAME)
        assert not res_mismatch.condition_was_satisfied
        assert d["k"] == "v2"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_different_updates_and_rejects_match(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res_match = d.set_item_if("k", "v2", etag, ETAG_HAS_CHANGED)
        assert not res_match.condition_was_satisfied
        assert d["k"] == "v1"

        res = d.set_item_if("k", "v3", mismatched_etag(spec, etag), ETAG_HAS_CHANGED)
        assert res.condition_was_satisfied
        assert d["k"] == "v3"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert not d.discard_item_if("k", mismatched_etag(spec, etag), ETAG_IS_THE_SAME).condition_was_satisfied
        assert "k" in d

        assert d.discard_item_if("k", etag, ETAG_IS_THE_SAME).condition_was_satisfied
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_different_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert not d.discard_item_if("k", etag, ETAG_HAS_CHANGED).condition_was_satisfied
        assert "k" in d

        assert d.discard_item_if("k", mismatched_etag(spec, etag), ETAG_HAS_CHANGED).condition_was_satisfied
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        assert not d.discard_item_if("missing", "etag", ETAG_IS_THE_SAME).condition_was_satisfied

        d["k"] = "v1"
        etag = d.etag("k")
        assert not d.discard_item_if("k", mismatched_etag(spec, etag), ETAG_IS_THE_SAME).condition_was_satisfied
        assert d.discard_item_if("k", etag, ETAG_IS_THE_SAME).condition_was_satisfied
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_different_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        # "etag" != ITEM_NOT_AVAILABLE => condition satisfied for missing key
        assert d.discard_item_if("missing", "etag", ETAG_HAS_CHANGED).condition_was_satisfied

        d["k"] = "v1"
        etag = d.etag("k")
        assert not d.discard_item_if("k", etag, ETAG_HAS_CHANGED).condition_was_satisfied
        assert d.discard_item_if("k", mismatched_etag(spec, etag), ETAG_HAS_CHANGED).condition_was_satisfied
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_equal_jokers(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.set_item_if("k", KEEP_CURRENT, etag, ETAG_IS_THE_SAME).condition_was_satisfied
        current_etag = d.etag("k")

        assert d.set_item_if("k", DELETE_CURRENT, current_etag, ETAG_IS_THE_SAME).condition_was_satisfied
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
@pytest.mark.parametrize(
    "method_name,args",
    [
        ("get_item_if", ("e", ETAG_HAS_CHANGED)),
        ("get_item_if", ("e", ETAG_IS_THE_SAME)),
        ("set_item_if", ("v", "e", ETAG_IS_THE_SAME)),
        ("set_item_if", ("v", "e", ETAG_HAS_CHANGED)),
        ("discard_item_if", ("e", ETAG_IS_THE_SAME)),
        ("discard_item_if", ("e", ETAG_HAS_CHANGED)),
    ],
)
def test_conditional_ops_missing_key_returns_item_not_available(tmp_path, spec, method_name, args):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        method = getattr(d, method_name)
        result = method("missing", *args)
        assert isinstance(result, ConditionalOperationResult)
        assert result.actual_etag is ITEM_NOT_AVAILABLE
