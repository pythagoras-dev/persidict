import pytest
from moto import mock_aws

from persidict import BasicS3Dict


@mock_aws
def test_basic_s3_setdefault_existing_key_ignores_invalid_default_type():
    d = BasicS3Dict(
        bucket_name="basic-setdefault-bucket",
        serialization_format="json",
        base_class_for_values=int,
    )
    d["k"] = 1

    result = d.setdefault("k", "bad")

    assert result == 1
    assert d["k"] == 1


@mock_aws
def test_basic_s3_setdefault_missing_key_rejects_invalid_default_type():
    d = BasicS3Dict(
        bucket_name="basic-setdefault-bucket",
        serialization_format="json",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError):
        d.setdefault("missing", "bad")
