from parameterizable import *


def test_file_dir_dict_registration():
    from src.persidict import FileDirDict
    assert is_registered(FileDirDict)
    smoketest_parameterizable_class(FileDirDict)


def test_s3_dict_registration():
    from src.persidict import S3Dict
    assert is_registered(S3Dict)
    smoketest_parameterizable_class(S3Dict)


def test_keep_current_registration():
    from src.persidict import KeepCurrentFlag
    assert is_registered(KeepCurrentFlag)
    smoketest_parameterizable_class(KeepCurrentFlag)


def test_delete_current_registration():
    from src.persidict import DeleteCurrentFlag
    assert is_registered(DeleteCurrentFlag)
    smoketest_parameterizable_class(DeleteCurrentFlag)