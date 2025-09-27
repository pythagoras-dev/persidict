import inspect

import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


def demo_function(a:int=0, b:str="", c:float=0.0, d:bool=False):
    for element in [a,b,c,d]:
        print(element)
    return str(a)+str(b)+str(c)+str(d)

@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_work_with_python_src(tmpdir, DictToTest, kwargs):
    """Validate how dict_to_test works with Python source code."""
    new_kwargs = dict(**kwargs)
    new_kwargs |= dict(base_dir=tmpdir, serialization_format = "py"
                       , base_class_for_values = str)

    dict_to_test = DictToTest(**new_kwargs)
    dict_to_test.clear()
    model_dict = dict()

    src = inspect.getsource(demo_function)
    print(f"{type(src)=}")
    dict_to_test["my_function"] = src
    assert dict_to_test["my_function"] == src

    dict_to_test.clear()

