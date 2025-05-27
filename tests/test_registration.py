# from persidict import FileDirDict

def test_file_dir_dict_registration():
    from parameterizable import is_registered
    from src.persidict import FileDirDict
    assert is_registered(FileDirDict)

def test_s3_dict_registration():
    from parameterizable import is_registered
    from src.persidict import S3Dict
    assert is_registered(S3Dict)