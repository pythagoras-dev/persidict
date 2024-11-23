import inspect
import random

import pytest
from moto import mock_aws

from persidict import FileDirDict, S3Dict, SafeStrTuple
import pandas as pd

from persidict.tests.data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_delete_if_exists(tmpdir, DictToTest, kwargs, rundom=None):
    dict_to_test = DictToTest(dir_name=tmpdir, **kwargs)
    dict_to_test.clear()

    good_keys = []
    bad_keys = []

    for i in range(1,12):
        good_k = ("good",)*i
        bad_k = ("bad",)*i
        good_keys.append(good_k)
        bad_keys.append(bad_k)
        dict_to_test[good_k] = i

    num_successful_deletions = 0
    all_keys = good_keys + bad_keys
    random.shuffle(all_keys)
    for k in all_keys:
        num_successful_deletions += dict_to_test.delete_if_exists(k)

    assert num_successful_deletions == len(good_keys)
    dict_to_test.clear()