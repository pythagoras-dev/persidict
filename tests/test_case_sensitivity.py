import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_case_sensitivity(tmpdir, DictToTest, kwargs):

    if "digest_len" in kwargs and kwargs["digest_len"] <=3:
        return

    dict_to_test = DictToTest(base_dir = tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()

    for s in ["aaAA", "AAaa", "AAAA", "aaaa", "aAaA", "aAaa"]:
        dict_to_test[s] = s + s
        model_dict[s] = s + s
        assert len(dict_to_test) == len(model_dict)
        assert dict_to_test[s] == model_dict[s]

    dict_to_test.clear()
