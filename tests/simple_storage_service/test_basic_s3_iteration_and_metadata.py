import pytest
from moto import mock_aws

from persidict import BasicS3Dict, SafeStrTuple


@mock_aws
def test_basic_s3_base_url_and_empty_subdict():
    d = BasicS3Dict(bucket_name="metadata-bucket", root_prefix="root")

    assert d.root_prefix.endswith("/")
    assert d.base_url == "s3://metadata-bucket/root/"

    sub = d.get_subdict(())
    assert sub.root_prefix == d.root_prefix
    assert sub.base_url == d.base_url


@mock_aws
def test_basic_s3_region_specific_bucket_creation():
    d = BasicS3Dict(bucket_name="regional-bucket", region="us-west-2")
    d["k"] = "v"

    assert d["k"] == "v"


@mock_aws
def test_basic_s3_len_and_keys_skip_non_matching_suffix():
    d = BasicS3Dict(bucket_name="iter-bucket", serialization_format="json")
    d["good"] = {"a": 1}

    d.s3_client.put_object(
        Bucket=d.bucket_name,
        Key=f"{d.root_prefix}raw.txt",
        Body=b"raw",
    )

    assert len(d) == 1
    assert list(d.keys()) == [SafeStrTuple("good")]


@mock_aws
def test_basic_s3_timestamp_missing_key_raises():
    d = BasicS3Dict(bucket_name="timestamp-bucket")

    with pytest.raises(KeyError):
        d.timestamp("missing")
