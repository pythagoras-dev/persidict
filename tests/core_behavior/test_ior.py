"""Tests for the __ior__ (|=) operator on PersiDict implementations.

The __ior__ method corresponds to the in-place OR operator (|=).
It should behave like dict.update() but returns self and is restricted
to Mapping arguments only (per implementation).
"""

import pytest
from moto import mock_aws

from persidict import LocalDict
from tests.data_for_mutable_tests import mutable_tests, make_test_dict



@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_ior_overwrites_existing(tmpdir, DictToTest, kwargs):
    """Test |= operator overwrites existing keys."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d.clear()
    
    d[("key",)] = "old_value"
    
    d |= {("key",): "new_value"}
    
    assert d[("key",)] == "new_value"
    
    d.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_ior_with_another_persidict(tmpdir, DictToTest, kwargs):
    """Test |= operator with another PersiDict."""
    d1 = make_test_dict(DictToTest, tmpdir, **kwargs)
    d1.clear()
    d1[("key1",)] = "val1"
    
    # Use LocalDict for source to avoid conflict/setup complexity
    d2 = LocalDict(serialization_format="pkl")
    d2[("key2",)] = "val2"
    d2[("key1",)] = "overwritten"
    
    d1 |= d2
    
    assert d1[("key1",)] == "overwritten"
    assert d1[("key2",)] == "val2"
    
    d1.clear()


