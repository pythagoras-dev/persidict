"""Tests for storing and retrieving pyarrow atomic types in PersiDict."""

import pytest
import pyarrow as pa

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pyarrow_array(tmp_path, DictToTest):
    """Verify pyarrow Array values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pa.array([1, 2, 3, 4, 5])
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pyarrow_table(tmp_path, DictToTest):
    """Verify pyarrow Table values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pyarrow_recordbatch(tmp_path, DictToTest):
    """Verify pyarrow RecordBatch values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = pa.record_batch({"a": [1, 2, 3], "b": [4, 5, 6]})
    d["key"] = original
    assert d["key"].equals(original)

    d.clear()
