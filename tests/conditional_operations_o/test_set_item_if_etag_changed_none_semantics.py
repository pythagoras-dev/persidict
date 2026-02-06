from __future__ import annotations

from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import ETAG_HAS_NOT_CHANGED, ETAG_UNKNOWN


def test_set_item_if_etag_changed_unknown_local():
    d = LocalDict(serialization_format="json")
    d["k"] = "v1"

    res = d.set_item_if_etag_changed("k", "v2", ETAG_UNKNOWN)

    assert res is not ETAG_HAS_NOT_CHANGED
    assert d["k"] == "v2"


def test_set_item_if_etag_changed_unknown_file(tmp_path):
    d = FileDirDict(base_dir=str(tmp_path / "file"), serialization_format="json")
    d["k"] = "v1"

    res = d.set_item_if_etag_changed("k", "v2", ETAG_UNKNOWN)

    assert res is not ETAG_HAS_NOT_CHANGED
    assert d["k"] == "v2"


@mock_aws
def test_set_item_if_etag_changed_unknown_basic_s3():
    d = BasicS3Dict(bucket_name="etag-none-basic", serialization_format="json")
    d["k"] = "v1"

    res = d.set_item_if_etag_changed("k", "v2", ETAG_UNKNOWN)

    assert res is not ETAG_HAS_NOT_CHANGED
    assert d["k"] == "v2"


@mock_aws
def test_set_item_if_etag_changed_unknown_s3_cached(tmp_path):
    d = S3Dict_FileDirCached(
        base_dir=str(tmp_path / "cache"),
        bucket_name="etag-none-cached",
        serialization_format="json",
    )
    d["k"] = "v1"

    res = d.set_item_if_etag_changed("k", "v2", ETAG_UNKNOWN)

    assert res is not ETAG_HAS_NOT_CHANGED
    assert d["k"] == "v2"
