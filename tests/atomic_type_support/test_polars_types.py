"""Tests for storing and retrieving polars atomic types in PersiDict."""

import pytest
import polars as pl

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_polars_dataframe(tmp_path, DictToTest):
    """Verify polars DataFrame values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_polars_series(tmp_path, DictToTest):
    """Verify polars Series values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pl.Series("x", [1, 2, 3, 4, 5])
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()
