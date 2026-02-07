import pytest
from moto import mock_aws

from tests.data_for_mutable_tests import mutable_tests_digest_len


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests_digest_len)
@mock_aws
def test_digest_length_roundtrip(tmpdir, DictToTest, kwargs):
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    d["Key"] = "value"
    assert d["Key"] == "value"

    if "digest_len" in kwargs:
        assert d.digest_len == kwargs["digest_len"]

    d["Key"] = "value2"
    assert d["Key"] == "value2"

    d.clear()
