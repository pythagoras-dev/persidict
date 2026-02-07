"""Tests for storing and retrieving sympy atomic types in PersiDict."""

import pytest
import sympy

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_sympy_symbol(tmp_path, DictToTest):
    """Verify sympy Symbol values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = sympy.Symbol("x")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_sympy_integer(tmp_path, DictToTest):
    """Verify sympy Integer values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = sympy.Integer(42)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_sympy_rational(tmp_path, DictToTest):
    """Verify sympy Rational values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = sympy.Rational(3, 7)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_sympy_float(tmp_path, DictToTest):
    """Verify sympy Float values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = sympy.Float(3.14159)
    d["key"] = original
    assert d["key"] == original

    d.clear()
