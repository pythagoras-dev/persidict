"""Tests for storing and retrieving pandas atomic types in PersiDict."""

import pytest

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pandas_dataframe(tmp_path, DictToTest):
    """Verify pandas DataFrame values can be stored and retrieved."""
    pd = pytest.importorskip("pandas")
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pandas_series(tmp_path, DictToTest):
    """Verify pandas Series values can be stored and retrieved."""
    pd = pytest.importorskip("pandas")
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pd.Series([1, 2, 3, 4, 5], name="test")
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pandas_index(tmp_path, DictToTest):
    """Verify pandas Index values can be stored and retrieved."""
    pd = pytest.importorskip("pandas")
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pd.Index([10, 20, 30, 40, 50])
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pandas_timestamp(tmp_path, DictToTest):
    """Verify pandas Timestamp values can be stored and retrieved."""
    pd = pytest.importorskip("pandas")
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pd.Timestamp("2024-01-15 12:30:45")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pandas_timedelta(tmp_path, DictToTest):
    """Verify pandas Timedelta values can be stored and retrieved."""
    pd = pytest.importorskip("pandas")
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pd.Timedelta("5 days 3 hours")
    d["key"] = original
    assert d["key"] == original

    d.clear()
