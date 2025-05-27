import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_iterators(tmpdir, DictToTest, kwargs):
    """Test if iterators work correctly."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    assert len(dict_to_test) == len(model_dict) == 0

    for i in range(25):
        k = f"key_{i*i}"
        dict_to_test[k] = 2*i
        model_dict[k] = 2*i

    assert (len(model_dict)
            == len(list(dict_to_test.keys()))
            == len(list(dict_to_test.values()))
            == len(list(dict_to_test.items())))

    assert sorted([str(k[0]) for k in dict_to_test.keys()]) == sorted(
        [str(k) for k in model_dict.keys()]) ##?!?!?!?!?!?!?
    assert sorted([str(v) for v in dict_to_test.values()]) == sorted(
        [str(v) for v in model_dict.values()])

    dict_to_test.clear()
