import pytest
from moto import mock_aws

import pandas as pd

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_work_with_pandas(tmpdir, DictToTest, kwargs):
    """Validate how dict_to_test works with various pandas data types."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()

    d = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    dict_to_test["z"] = d
    model_dict["z"] = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    assert (dict_to_test["z"] == model_dict["z"]).sum().sum() == 5

    dict_to_test["zz"] = pd.Series([1, 2, 3, 4, 5, 6])
    model_dict["zz"] = pd.Series([1, 2, 3, 4, 5, 6])
    assert (dict_to_test["zz"] == model_dict["zz"]).sum() == 6

    dict_to_test["zzz"] = pd.Index([1, 2, 3, 4])
    model_dict["zzz"] = pd.Index([1, 2, 3, 4])
    assert (dict_to_test["zzz"] == model_dict["zzz"]).sum() == 4

    dict_to_test["zzzz"] = pd.RangeIndex(0, 15)
    model_dict["zzzz"] = pd.RangeIndex(0, 15)
    assert (dict_to_test["zzzz"] == model_dict["zzzz"]).sum() == 15

    #TODO: add MultiIndex tests

    dict_to_test.clear()
