from moto import mock_aws
import pytest

from persidict import BasicS3Dict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import (
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT,
)


@mock_aws
def test_s3_append_only_etag_uses_native_etag(tmp_path):
    d = S3Dict_FileDirCached(
        bucket_name="append-only-bucket",
        base_dir=str(tmp_path),
        serialization_format="json",
        append_only=True,
    )

    d["k"] = "v"

    etag = d.etag("k")
    main_etag = d._main_dict.etag("k")

    assert etag == main_etag


@mock_aws
def test_s3_append_only_insert_if_absent_succeeds():
    """Insert-only conditional write on an absent key should succeed."""
    d = BasicS3Dict(
        bucket_name="ao-insert-bucket",
        serialization_format="json",
        append_only=True,
    )

    result = d.set_item_if("k", "v1", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert d["k"] == "v1"
    assert result.resulting_etag is not ITEM_NOT_AVAILABLE


@mock_aws
def test_s3_append_only_insert_if_absent_rejects_duplicate():
    """Insert-only conditional write on an existing key must not overwrite."""
    d = BasicS3Dict(
        bucket_name="ao-dup-bucket",
        serialization_format="json",
        append_only=True,
    )
    d["k"] = "original"

    result = d.set_item_if("k", "replacement", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert d["k"] == "original"


@mock_aws
def test_s3_append_only_overwrite_with_matching_etag_blocked():
    """Even with a matching etag, overwriting an existing key is forbidden
    in append-only mode."""
    d = BasicS3Dict(
        bucket_name="ao-overwrite-bucket",
        serialization_format="json",
        append_only=True,
    )
    d["k"] = "v1"
    etag = d.etag("k")

    with pytest.raises(KeyError):
        d.set_item_if("k", "v2", etag, ETAG_IS_THE_SAME)

    assert d["k"] == "v1"


@mock_aws
def test_s3_append_only_keep_current_allowed_on_existing():
    """KEEP_CURRENT is a no-op probe and should be allowed even in
    append-only mode on an existing key."""
    d = BasicS3Dict(
        bucket_name="ao-keep-bucket",
        serialization_format="json",
        append_only=True,
    )
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.set_item_if("k", KEEP_CURRENT, etag, ETAG_IS_THE_SAME)

    assert result.condition_was_satisfied
    assert d["k"] == "v1"
