import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests
from persidict import KEEP_CURRENT


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_keep_current(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    assert len(dict_to_test) == len(model_dict) == 0

    dict_to_test["test_key"] = KEEP_CURRENT
    assert len(dict_to_test) == 0

    all_keys = [("test",f"key_{i}","Q") for i in range(10)]

    for i,k in enumerate(all_keys):
        dict_to_test[k] = i
        dict_to_test[k] = KEEP_CURRENT
        model_dict[k] = i
        assert dict_to_test[k] == i
        assert len(dict_to_test) == len(model_dict)
        dict_to_test[k] = i+1
        model_dict[k] = i+1
        assert dict_to_test[k] == model_dict[k]

    for i,k in enumerate(all_keys):
        fake_k = f"fake_key_{i}"
        dict_to_test[fake_k] = KEEP_CURRENT
        assert k in dict_to_test
        assert fake_k not in dict_to_test
        del dict_to_test[k]
        del model_dict[k]
        assert len(dict_to_test) == len(model_dict)
        assert k not in dict_to_test

    dict_to_test.clear()
