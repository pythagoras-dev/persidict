import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_complex_keys(tmpdir, DictToTest, kwargs):
    """Test if compound keys work correctly."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()

    for k in [("a", "a_1"), ("a", "a_2"), ("b", "b_1", "b_2", "b_3")]:
        dict_to_test[k] = 2*str(k)
        model_dict[k] = 2*str(k)
        assert len(dict_to_test) == len(model_dict)
        assert dict_to_test[k] == model_dict[k]

    dict_to_test.clear()
