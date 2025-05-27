import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts(tmpdir, DictToTest, kwargs):
    """Test if get_subdict() works correctly."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()

    fdd = dict_to_test
    fdd[("a", "a_1")] = 10
    fdd[("a", "a_2")] = 100
    fdd[("b", "b_1")] = 1000
    assert len(fdd.get_subdict("a")) == 2
    assert len(fdd.get_subdict("b")) == 1
    assert len(fdd.get_subdict("f")) == 0
    assert len(fdd.get_subdict(("i","j","k"))) == 0
    sbscts = fdd.subdicts()
    assert len(sbscts) == 2

    fdd[("b", "b_2")] = 10000
    fdd[("b", "b_3")] = 100000
    fdd[("c", "c_1")] = None
    assert len(fdd.get_subdict("a")) == 2
    assert len(fdd.get_subdict("b")) == 3
    assert len(fdd.get_subdict("c")) == 1
    sbscts = fdd.subdicts()
    assert len(sbscts) == 3

    print(f"{fdd.__dict__}")

    fdd.clear()
    assert len(fdd.get_subdict("a")) == 0
    assert len(fdd.get_subdict("b")) == 0
