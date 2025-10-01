from parameterizable import *
from moto import mock_aws

def test_file_dir_dict_registration():
    from persidict import FileDirDict
    assert is_registered(FileDirDict)
    smoketest_parameterizable_class(FileDirDict)

@mock_aws
def test_s3_dict_registration():
    from persidict import S3Dict_Legacy
    assert is_registered(S3Dict_Legacy)
    smoketest_parameterizable_class(S3Dict_Legacy)