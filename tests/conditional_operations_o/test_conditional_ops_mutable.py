from __future__ import annotations

from contextlib import contextmanager

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import (
    DELETE_CURRENT,
    ETAG_HAS_CHANGED,
    ETAG_HAS_NOT_CHANGED,
    KEEP_CURRENT,
    EQUAL_ETAG,
    DIFFERENT_ETAG,
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

        assert d.get_item_if_etag("k", etag, DIFFERENT_ETAG) is ETAG_HAS_NOT_CHANGED

        value, new_etag = d.get_item_if_etag("k", mismatched_etag(spec, etag), DIFFERENT_ETAG)
        assert value == "v1"
        assert new_etag == d.etag("k")


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_get_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        value, new_etag = d.get_item_if_etag("k", etag, EQUAL_ETAG)
        assert value == "v1"
        assert new_etag == d.etag("k")

        assert d.get_item_if_etag("k", mismatched_etag(spec, etag), EQUAL_ETAG) is ETAG_HAS_CHANGED


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_equal_updates_and_rejects_mismatch(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res = d.set_item_if_etag("k", "v2", etag, EQUAL_ETAG)
        assert res is not ETAG_HAS_CHANGED
        assert d["k"] == "v2"
        if res is not None:
            assert res == d.etag("k")

        res_mismatch = d.set_item_if_etag("k", "v3", mismatched_etag(spec, etag), EQUAL_ETAG)
        assert res_mismatch is ETAG_HAS_CHANGED
        assert d["k"] == "v2"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_different_updates_and_rejects_match(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res_match = d.set_item_if_etag("k", "v2", etag, DIFFERENT_ETAG)
        assert res_match is ETAG_HAS_NOT_CHANGED
        assert d["k"] == "v1"

        res = d.set_item_if_etag("k", "v3", mismatched_etag(spec, etag), DIFFERENT_ETAG)
        assert res is not ETAG_HAS_NOT_CHANGED
        assert d["k"] == "v3"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.delete_item_if_etag("k", mismatched_etag(spec, etag), EQUAL_ETAG) is ETAG_HAS_CHANGED
        assert "k" in d

        assert d.delete_item_if_etag("k", etag, EQUAL_ETAG) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_different_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.delete_item_if_etag("k", etag, DIFFERENT_ETAG) is ETAG_HAS_NOT_CHANGED
        assert "k" in d

        assert d.delete_item_if_etag("k", mismatched_etag(spec, etag), DIFFERENT_ETAG) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_equal_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        assert d.discard_item_if_etag("missing", "etag", EQUAL_ETAG) is False

        d["k"] = "v1"
        etag = d.etag("k")
        assert d.discard_item_if_etag("k", mismatched_etag(spec, etag), EQUAL_ETAG) is False
        assert d.discard_item_if_etag("k", etag, EQUAL_ETAG) is True
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_different_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        assert d.discard_item_if_etag("missing", "etag", DIFFERENT_ETAG) is False

        d["k"] = "v1"
        etag = d.etag("k")
        assert d.discard_item_if_etag("k", etag, DIFFERENT_ETAG) is False
        assert d.discard_item_if_etag("k", mismatched_etag(spec, etag), DIFFERENT_ETAG) is True
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_equal_jokers(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.set_item_if_etag("k", KEEP_CURRENT, etag, EQUAL_ETAG) is None
        current_etag = d.etag("k")

        assert d.set_item_if_etag("k", DELETE_CURRENT, current_etag, EQUAL_ETAG) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
@pytest.mark.parametrize(
    "method_name,args",
    [
        ("get_item_if_etag", ("e", DIFFERENT_ETAG)),
        ("get_item_if_etag", ("e", EQUAL_ETAG)),
        ("set_item_if_etag", ("v", "e", EQUAL_ETAG)),
        ("set_item_if_etag", ("v", "e", DIFFERENT_ETAG)),
        ("delete_item_if_etag", ("e", EQUAL_ETAG)),
        ("delete_item_if_etag", ("e", DIFFERENT_ETAG)),
    ],
)
def test_conditional_ops_raise_on_missing_key(tmp_path, spec, method_name, args):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        method = getattr(d, method_name)
        with pytest.raises((KeyError, FileNotFoundError)):
            method("missing", *args)
