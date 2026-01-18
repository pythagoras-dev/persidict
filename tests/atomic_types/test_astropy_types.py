"""Tests for storing and retrieving astropy atomic types in PersiDict."""

import pytest
import numpy as np
from astropy import units as u

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_astropy_quantity(tmp_path, DictToTest):
    """Verify astropy Quantity values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = 5.0 * u.meter
    d["key"] = original
    retrieved = d["key"]
    assert retrieved.value == original.value
    assert retrieved.unit == original.unit

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_astropy_quantity_array(tmp_path, DictToTest):
    """Verify astropy Quantity with array values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = np.array([1.0, 2.0, 3.0]) * u.kilogram
    d["key"] = original
    retrieved = d["key"]
    assert np.array_equal(retrieved.value, original.value)
    assert retrieved.unit == original.unit

    d.clear()
