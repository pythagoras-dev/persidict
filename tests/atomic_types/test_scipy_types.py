"""Tests for storing and retrieving scipy.sparse atomic types in PersiDict."""

import pytest
import numpy as np
import scipy.sparse

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_scipy_csr_matrix(tmp_path, DictToTest):
    """Verify scipy csr_matrix values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = scipy.sparse.csr_matrix([[1, 2, 0], [0, 0, 3], [4, 0, 5]])
    d["key"] = original
    assert np.array_equal(d["key"].toarray(), original.toarray())

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_scipy_csc_matrix(tmp_path, DictToTest):
    """Verify scipy csc_matrix values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = scipy.sparse.csc_matrix([[1, 2, 0], [0, 0, 3], [4, 0, 5]])
    d["key"] = original
    assert np.array_equal(d["key"].toarray(), original.toarray())

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_scipy_coo_matrix(tmp_path, DictToTest):
    """Verify scipy coo_matrix values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = scipy.sparse.coo_matrix([[1, 2, 0], [0, 0, 3], [4, 0, 5]])
    d["key"] = original
    assert np.array_equal(d["key"].toarray(), original.toarray())

    d.clear()
