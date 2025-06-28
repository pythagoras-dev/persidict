import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_random_key(tmpdir, DictToTest, kwargs):
    # Test with empty dictionary
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    assert dict_to_test.random_key() is None

    # Test with non-empty dictionary
    for n in range(10):
        dict_to_test[str(n)] = n**2
        for i in range(3):
            random_key = dict_to_test.random_key()
            assert random_key is not None
            assert random_key in dict_to_test
