"""Tests that setdefault_if rejects Joker values as default_value.

KEEP_CURRENT and DELETE_CURRENT are command-like sentinels that make no
sense as a default value for setdefault_if. The method must raise TypeError
for both, across all backends.
"""

import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import (
    KEEP_CURRENT,
    DELETE_CURRENT,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


@pytest.mark.parametrize("joker", [KEEP_CURRENT, DELETE_CURRENT])
@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_rejects_joker_default(tmpdir, DictToTest, kwargs, joker):
    """setdefault_if raises TypeError when default_value is a Joker."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    with pytest.raises(TypeError):
        d.setdefault_if(
            "key",
            default_value=joker,
            condition=ETAG_IS_THE_SAME,
            expected_etag=ITEM_NOT_AVAILABLE,
        )


@pytest.mark.parametrize("joker", [KEEP_CURRENT, DELETE_CURRENT])
@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_if_rejects_joker_even_when_key_exists(
        tmpdir, DictToTest, kwargs, joker):
    """setdefault_if rejects Joker default_value regardless of key presence."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["key"] = "existing"
    etag = d.etag("key")

    with pytest.raises(TypeError):
        d.setdefault_if(
            "key",
            default_value=joker,
            condition=ETAG_IS_THE_SAME,
            expected_etag=etag,
        )
