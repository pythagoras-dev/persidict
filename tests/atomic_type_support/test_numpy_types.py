"""Tests for storing and retrieving numpy atomic types in PersiDict."""

import pytest

from ..atomic_test_config import atomic_type_tests, make_test_dict


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_ndarray_1d(tmp_path, DictToTest):
    """Verify numpy 1D array values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.array([1, 2, 3, 4, 5])
    d["key"] = original
    assert np.array_equal(d["key"], original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_ndarray_2d(tmp_path, DictToTest):
    """Verify numpy 2D array values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.array([[1, 2, 3], [4, 5, 6]])
    d["key"] = original
    assert np.array_equal(d["key"], original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_ndarray_float(tmp_path, DictToTest):
    """Verify numpy float array values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    d["key"] = original
    assert np.array_equal(d["key"], original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_int64(tmp_path, DictToTest):
    """Verify numpy.int64 values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.int64(42)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_float64(tmp_path, DictToTest):
    """Verify numpy.float64 values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.float64(3.14159)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_numpy_dtype(tmp_path, DictToTest):
    """Verify numpy.dtype values can be stored and retrieved."""
    np = pytest.importorskip("numpy")
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = np.dtype("float64")
    d["key"] = original
    assert d["key"] == original

    d.clear()
