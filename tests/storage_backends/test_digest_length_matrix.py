import pytest
from moto import mock_aws

from tests.data_for_mutable_tests import mutable_tests_digest_len, make_test_dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests_digest_len)
@mock_aws
def test_digest_length_roundtrip(tmpdir, DictToTest, kwargs):
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d.clear()

    d["Key"] = "value"
    assert d["Key"] == "value"

    if "digest_len" in kwargs:
        assert d.digest_len == kwargs["digest_len"]

    d["Key"] = "value2"
    assert d["Key"] == "value2"

    d.clear()
