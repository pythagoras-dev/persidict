import random

import pytest
from moto import mock_aws

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_discard(tmpdir, DictToTest, kwargs, rundom=None):
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()

    good_keys = []
    bad_keys = []

    for i in range(1, 12):
        good_k = ("good",) * i
        bad_k = ("bad",) * i
        good_keys.append(good_k)
        bad_keys.append(bad_k)
        d[good_k] = i

    num_successful_deletions = 0
    all_keys = good_keys + bad_keys
    random.shuffle(all_keys)
    for k in all_keys:
        num_successful_deletions += d.discard(k)

    assert num_successful_deletions == len(good_keys)
    d.clear()
