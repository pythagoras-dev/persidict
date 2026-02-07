"""Tests for the subdicts() method that returns first-level sub-dictionaries."""

import pytest
from moto import mock_aws

from persidict import PersiDict

from tests.data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts_returns_dict_of_subdicts(tmpdir, DictToTest, kwargs):
    """Verify subdicts() returns a dict mapping first-level keys to subdicts."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d[("users", "alice")] = "user_alice"
    d[("users", "bob")] = "user_bob"
    d[("logs", "entry1")] = "log1"

    result = d.subdicts()

    assert isinstance(result, dict)
    assert set(result.keys()) == {"users", "logs"}
    assert all(isinstance(v, PersiDict) for v in result.values())


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts_empty_dict_returns_empty(tmpdir, DictToTest, kwargs):
    """Verify subdicts() returns empty dict for empty dictionary."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.subdicts()

    assert result == {}


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts_single_toplevel_key(tmpdir, DictToTest, kwargs):
    """Verify subdicts() with single top-level key returns one entry."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d[("only_prefix", "key1")] = "value1"
    d[("only_prefix", "key2")] = "value2"

    result = d.subdicts()

    assert len(result) == 1
    assert "only_prefix" in result
    assert len(result["only_prefix"]) == 2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts_multiple_toplevel_keys(tmpdir, DictToTest, kwargs):
    """Verify subdicts() correctly groups items by first key segment."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d[("a", "1")] = 10
    d[("a", "2")] = 20
    d[("b", "1")] = 30
    d[("c", "1")] = 40
    d[("c", "2")] = 50
    d[("c", "3")] = 60

    result = d.subdicts()

    assert len(result) == 3
    assert len(result["a"]) == 2
    assert len(result["b"]) == 1
    assert len(result["c"]) == 3


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_subdicts_values_are_functional(tmpdir, DictToTest, kwargs):
    """Verify subdicts can be used to read and write values."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d[("prefix", "existing")] = "original"

    subs = d.subdicts()
    prefix_sub = subs["prefix"]

    # Read through subdict
    assert prefix_sub[("existing",)] == "original"

    # Write through subdict
    prefix_sub[("new_key",)] = "new_value"

    # Verify visible in parent
    assert d[("prefix", "new_key")] == "new_value"
    assert len(d) == 2
