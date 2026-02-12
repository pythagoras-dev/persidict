"""Tests for storing and retrieving built-in atomic types in PersiDict."""

import pytest

from ..atomic_test_config import atomic_type_tests, make_test_dict


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_str_type(tmp_path, DictToTest):
    """Verify string values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["key"] = "hello world"
    assert d["key"] == "hello world"

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_bytes_type(tmp_path, DictToTest):
    """Verify bytes values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["key"] = b"binary data"
    assert d["key"] == b"binary data"

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_bytearray_type(tmp_path, DictToTest):
    """Verify bytearray values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = bytearray(b"mutable bytes")
    d["key"] = original
    retrieved = d["key"]
    assert retrieved == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_int_type(tmp_path, DictToTest):
    """Verify integer values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["positive"] = 42
    d["negative"] = -100
    d["zero"] = 0
    d["large"] = 10**100

    assert d["positive"] == 42
    assert d["negative"] == -100
    assert d["zero"] == 0
    assert d["large"] == 10**100

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_float_type(tmp_path, DictToTest):
    """Verify float values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["pi"] = 3.14159
    d["negative"] = -2.5
    d["zero"] = 0.0

    assert d["pi"] == 3.14159
    assert d["negative"] == -2.5
    assert d["zero"] == 0.0

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_complex_type(tmp_path, DictToTest):
    """Verify complex number values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["key"] = complex(3, 4)
    assert d["key"] == complex(3, 4)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_bool_type(tmp_path, DictToTest):
    """Verify boolean values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["true"] = True
    d["false"] = False

    assert d["true"] is True
    assert d["false"] is False

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_none_type(tmp_path, DictToTest):
    """Verify None values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["key"] = None
    assert d["key"] is None

    d.clear()
