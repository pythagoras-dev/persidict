import pytest
from moto import mock_aws

from persidict import SafeStrTuple

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_more_dict_methods(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()

    for i in range(10):
        k = ("_"+str(10*i),)
        k = SafeStrTuple(k)
        dict_to_test[k] = i + 1
        model_dict[k] = i + 1

        k = (i + 1) * (str(i) + "zz",)
        k = SafeStrTuple(k)
        fake_k = (i + 1) * (str(i) + "aa",)
        fake_k = SafeStrTuple(fake_k)
        dict_to_test[k] = "hihi"
        model_dict[k] = "hihi"

        assert k in dict_to_test
        assert fake_k not in dict_to_test
        assert SafeStrTuple(k+"UUUU") not in dict_to_test

        new_key = ("new_key", str(i))
        new_key = SafeStrTuple(new_key)

        assert (dict_to_test.setdefault(new_key, 1) ==
                model_dict.setdefault(new_key, 1))

        assert (dict_to_test.setdefault(new_key, 2) ==
                model_dict.setdefault(new_key, 2))

        assert (dict_to_test.pop(new_key, 2) ==
                model_dict.pop(new_key, 2))

    assert dict_to_test == dict_to_test
    assert dict_to_test == model_dict

    for v in model_dict:
        assert v in dict_to_test

    dict_to_test.clear()
