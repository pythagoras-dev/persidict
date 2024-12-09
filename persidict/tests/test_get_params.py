import inspect
import random
from copy import deepcopy

import pytest
from moto import mock_aws

from persidict import FileDirDict, S3Dict, SafeStrTuple
import pandas as pd

from parameterizable import CLASSNAME_PARAM_KEY
from persidict.tests.data_for_mutable_tests import mutable_tests

@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_portable_params(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_params = DictToTest.__get_portable_default_params__()
    model_params.update(kwargs)
    model_params["base_dir"] = str(tmpdir)

    assert model_params[CLASSNAME_PARAM_KEY] == DictToTest.__name__

    if "root_prefix" in model_params:
        if isinstance(model_params["root_prefix"], str):
            if len(model_params["root_prefix"]) > 0:
                if model_params["root_prefix"][-1] != "/":
                    model_params["root_prefix"] += "/"

    params = dict_to_test.__get_portable_params__()
    assert isinstance(params, dict)
    assert params == model_params


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_get_default_portable_params(tmpdir, DictToTest, kwargs):
    dict_to_test_1 = DictToTest().__get_portable_params__()
    dict_to_test_2 = DictToTest.__get_portable_default_params__()
    assert dict_to_test_1 == dict_to_test_2