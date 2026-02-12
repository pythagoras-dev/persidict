import pytest
from moto import mock_aws

from tests.data_for_mutable_tests import mutable_tests_root_prefix, make_test_dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests_root_prefix)
@mock_aws
def test_root_prefix_roundtrip(tmpdir, DictToTest, kwargs):
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d.clear()

    assert d.root_prefix.endswith("/")

    d["rooted"] = "value"
    assert d["rooted"] == "value"

    d.clear()
