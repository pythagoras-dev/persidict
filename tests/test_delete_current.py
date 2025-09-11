import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests
from persidict import DELETE_CURRENT


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_current(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    assert len(dict_to_test) == len(model_dict) == 0

    dict_to_test["test_key"] = 12345
    assert len(dict_to_test) == 1
    dict_to_test["test_key"] = DELETE_CURRENT
    assert len(dict_to_test) == 0
    dict_to_test["test_key"] = DELETE_CURRENT
    assert len(dict_to_test) == 0

    all_keys = [("test",f"key_{i}","Q") for i in range(10)]

    for i,k in enumerate(all_keys):
        dict_to_test[k] = i
        dict_to_test[k] = DELETE_CURRENT
        assert k not in dict_to_test

    dict_to_test.clear()
