import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests
from persidict import FileDirDict, S3Dict


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_base_url(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()

    assert isinstance(dict_to_test.base_url, str)

    if isinstance(dict_to_test, FileDirDict):
        assert dict_to_test.base_url.startswith("file:///")
        assert dict_to_test.base_url.endswith(dict_to_test._base_dir)

    if isinstance(dict_to_test, S3Dict):
        assert dict_to_test.base_url.startswith("s3://")
        assert dict_to_test.bucket_name in dict_to_test.base_url
        dict_to_test.base_url.endswith(dict_to_test.root_prefix)




