import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_random_keys(tmpdir, DictToTest, kwargs):
    dict_to_test = DictToTest(base_dir = tmpdir, **kwargs)
    for n in range(10):
        dict_to_test[str(n)] = n**2
        dict_len = len(dict_to_test)
        for i in range(dict_len+1):
            assert len(dict_to_test.random_keys(max_n=i)) == i
        for i in range(dict_len+1, 10):
            assert len(dict_to_test.random_keys(max_n=i)) == dict_len

    for q in range(1,6):
        all_keys = set()
        for n in range(50):
            single_random_key = dict_to_test.random_keys(max_n=q)[0]
            all_keys |= {single_random_key}
        assert len(all_keys) >= 7
