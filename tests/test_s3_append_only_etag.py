from moto import mock_aws

from persidict import S3Dict_FileDirCached


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
