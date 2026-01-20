"""Expanded tests for setdefault() across all backends."""

import pytest
from moto import mock_aws

from persidict.jokers_and_status_flags import KEEP_CURRENT, DELETE_CURRENT

from .data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_on_missing_key_stores_default(tmpdir, DictToTest, kwargs):
    """Verify setdefault stores and returns default when key is absent."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault("missing_key", "default_value")

    assert result == "default_value"
    assert d["missing_key"] == "default_value"
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_on_existing_key_returns_current(tmpdir, DictToTest, kwargs):
    """Verify setdefault returns existing value without modifying it."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["existing_key"] = "original_value"

    result = d.setdefault("existing_key", "ignored_default")

    assert result == "original_value"
    assert d["existing_key"] == "original_value"
    assert len(d) == 1


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_with_none_default(tmpdir, DictToTest, kwargs):
    """Verify setdefault works correctly with None as default value."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault("key1", None)

    assert result is None
    assert d["key1"] is None


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_rejects_keep_current_joker(tmpdir, DictToTest, kwargs):
    """Verify setdefault raises TypeError when default is KEEP_CURRENT."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises(TypeError):
        d.setdefault("key1", KEEP_CURRENT)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_rejects_delete_current_joker(tmpdir, DictToTest, kwargs):
    """Verify setdefault raises TypeError when default is DELETE_CURRENT."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    with pytest.raises(TypeError):
        d.setdefault("key1", DELETE_CURRENT)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_with_complex_keys(tmpdir, DictToTest, kwargs):
    """Verify setdefault works with tuple keys."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault(("prefix", "subkey"), {"nested": "data"})

    assert result == {"nested": "data"}
    assert d[("prefix", "subkey")] == {"nested": "data"}


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_with_default_omitted(tmpdir, DictToTest, kwargs):
    """Verify setdefault uses None when default is omitted."""
    d = DictToTest(base_dir=tmpdir, **kwargs)

    result = d.setdefault("key1")

    assert result is None
    assert d["key1"] is None


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_setdefault_does_not_mutate_stored_value(tmpdir, DictToTest, kwargs):
    """Verify mutating returned object doesn't affect stored value (for json)."""
    if kwargs.get("serialization_format") != "json":
        pytest.skip("Mutation test only relevant for json serialization")

    d = DictToTest(base_dir=tmpdir, **kwargs)
    original = {"x": 1}

    returned = d.setdefault("key1", original)
    returned["y"] = 2

    # The stored value should still be the original
    assert d["key1"] == {"x": 1}
