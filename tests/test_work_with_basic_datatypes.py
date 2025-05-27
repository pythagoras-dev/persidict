import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_work_with_basic_datatypes(tmpdir, DictToTest, kwargs):
    sample_data = [ [1,2,3,4,5]
                    ,["a","b","c","d","e"]
                    ,[i*i/3.14 for i in range(55)]
                    ,[str(i)*i for i in range(33)]
                    ,(1,2,3,4,5)
                    ,{"a":1,"b":2,"c":3,"d":4,"e":5}
                    ,{1,2,3,4,5}
                    ,True
                    ,(1,2,3,4,5,(6,7,8,9,10,(11,12,13,14,15)))
                    ]
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    for i,d in enumerate(sample_data):
        dict_to_test[str(i)] = d
        model_dict[str(i)] = d
        assert len(dict_to_test) == len(model_dict)
        assert dict_to_test[str(i)] == model_dict[str(i)]

    assert dict_to_test == model_dict

    dict_to_test.clear()
