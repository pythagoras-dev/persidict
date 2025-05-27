import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_basics(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    assert len(dict_to_test) == len(model_dict) == 0

    all_keys = [("test",f"key_{i}","Q") for i in range(10)]

    for i,k in enumerate(all_keys):
        dict_to_test[k] = i
        dict_to_test[k] = i
        model_dict[k] = i
        assert len(dict_to_test) == len(model_dict)
        dict_to_test[k] = i+1
        model_dict[k] = i+1
        assert dict_to_test[k] == model_dict[k]

    for i,k in enumerate(all_keys):
        fake_k = f"fake_key_{i}"
        assert k in dict_to_test
        assert fake_k not in dict_to_test
        del dict_to_test[k]
        del model_dict[k]
        assert len(dict_to_test) == len(model_dict)
        assert k not in dict_to_test

    dict_to_test.clear()
