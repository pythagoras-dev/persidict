"""Tests for storing and retrieving shapely atomic types in PersiDict."""

import pytest
import shapely

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_shapely_point(tmp_path, DictToTest):
    """Verify shapely Point values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = shapely.Point(1.0, 2.0)
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_shapely_polygon(tmp_path, DictToTest):
    """Verify shapely Polygon values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = shapely.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_shapely_linestring(tmp_path, DictToTest):
    """Verify shapely LineString values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = shapely.LineString([(0, 0), (1, 1), (2, 0)])
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()
