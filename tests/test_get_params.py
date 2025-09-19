import pytest
from moto import mock_aws

from parameterizable import CLASSNAME_PARAM_KEY
from data_for_mutable_tests import mutable_tests

@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_portable_params(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_params = DictToTest.get_portable_default_params()
    model_params.update(kwargs)
    if "base_dir" in model_params:
        model_params["base_dir"] = str(tmpdir)

    assert model_params[CLASSNAME_PARAM_KEY] == DictToTest.__name__

    if "root_prefix" in model_params:
        if isinstance(model_params["root_prefix"], str):
            if len(model_params["root_prefix"]) > 0:
                if model_params["root_prefix"][-1] != "/":
                    model_params["root_prefix"] += "/"

    params = dict_to_test.get_portable_params()
    assert isinstance(params, dict)
    assert params == model_params


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_default_portable_params(tmpdir, DictToTest, kwargs):
    dict_to_test_1 = DictToTest().get_portable_params()
    dict_to_test_2 = DictToTest.get_portable_default_params()
    assert dict_to_test_1 == dict_to_test_2
