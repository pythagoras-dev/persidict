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
def test_get_item_if_etag_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.get_item_if_etag_changed("k", etag) is ETAG_HAS_NOT_CHANGED

        value, new_etag = d.get_item_if_etag_changed("k", mismatched_etag(spec, etag))
        assert value == "v1"
        assert new_etag == d.etag("k")


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_get_item_if_etag_not_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        value, new_etag = d.get_item_if_etag_not_changed("k", etag)
        assert value == "v1"
        assert new_etag == d.etag("k")

        assert d.get_item_if_etag_not_changed("k", mismatched_etag(spec, etag)) is ETAG_HAS_CHANGED


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_not_changed_updates_and_rejects_mismatch(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res = d.set_item_if_etag_not_changed("k", "v2", etag)
        assert res is not ETAG_HAS_CHANGED
        assert d["k"] == "v2"
        if res is not None:
            assert res == d.etag("k")

        res_mismatch = d.set_item_if_etag_not_changed("k", "v3", mismatched_etag(spec, etag))
        assert res_mismatch is ETAG_HAS_CHANGED
        assert d["k"] == "v2"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_changed_updates_and_rejects_match(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        res_match = d.set_item_if_etag_changed("k", "v2", etag)
        assert res_match is ETAG_HAS_NOT_CHANGED
        assert d["k"] == "v1"

        res = d.set_item_if_etag_changed("k", "v3", mismatched_etag(spec, etag))
        assert res is not ETAG_HAS_NOT_CHANGED
        assert d["k"] == "v3"


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_not_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.delete_item_if_etag_not_changed("k", mismatched_etag(spec, etag)) is ETAG_HAS_CHANGED
        assert "k" in d

        assert d.delete_item_if_etag_not_changed("k", etag) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_delete_item_if_etag_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.delete_item_if_etag_changed("k", etag) is ETAG_HAS_NOT_CHANGED
        assert "k" in d

        assert d.delete_item_if_etag_changed("k", mismatched_etag(spec, etag)) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_not_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        assert d.discard_item_if_etag_not_changed("missing", "etag") is False

        d["k"] = "v1"
        etag = d.etag("k")
        assert d.discard_item_if_etag_not_changed("k", mismatched_etag(spec, etag)) is False
        assert d.discard_item_if_etag_not_changed("k", etag) is True
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_discard_item_if_etag_changed_respects_etag(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        assert d.discard_item_if_etag_changed("missing", "etag") is False

        d["k"] = "v1"
        etag = d.etag("k")
        assert d.discard_item_if_etag_changed("k", etag) is False
        assert d.discard_item_if_etag_changed("k", mismatched_etag(spec, etag)) is True
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
def test_set_item_if_etag_not_changed_jokers(tmp_path, spec):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        d["k"] = "v1"
        etag = d.etag("k")

        assert d.set_item_if_etag_not_changed("k", KEEP_CURRENT, etag) is None
        current_etag = d.etag("k")

        assert d.set_item_if_etag_not_changed("k", DELETE_CURRENT, current_etag) is None
        assert "k" not in d


@pytest.mark.parametrize("spec", MUTABLE_SPECS, ids=[s["name"] for s in MUTABLE_SPECS])
@pytest.mark.parametrize(
    "method_name,args",
    [
        ("get_item_if_etag_changed", ("e",)),
        ("get_item_if_etag_not_changed", ("e",)),
        ("set_item_if_etag_not_changed", ("v", "e")),
        ("set_item_if_etag_changed", ("v", "e")),
        ("delete_item_if_etag_not_changed", ("e",)),
        ("delete_item_if_etag_changed", ("e",)),
    ],
)
def test_conditional_ops_raise_on_missing_key(tmp_path, spec, method_name, args):
    with maybe_mock_aws(spec["uses_s3"]):
        d = build_dict(spec, tmp_path)
        method = getattr(d, method_name)
        with pytest.raises((KeyError, FileNotFoundError)):
            method("missing", *args)
