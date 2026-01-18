"""Tests for storing and retrieving torch atomic types in PersiDict."""

import pytest

torch = pytest.importorskip("torch", reason="PyTorch not available on this platform")

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_torch_tensor_1d(tmp_path, DictToTest):
    """Verify torch 1D Tensor values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = torch.tensor([1, 2, 3, 4, 5])
    d["key"] = original
    assert torch.equal(d["key"], original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_torch_tensor_2d(tmp_path, DictToTest):
    """Verify torch 2D Tensor values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = torch.tensor([[1, 2, 3], [4, 5, 6]])
    d["key"] = original
    assert torch.equal(d["key"], original)

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_torch_tensor_float(tmp_path, DictToTest):
    """Verify torch float Tensor values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = torch.tensor([1.1, 2.2, 3.3, 4.4, 5.5])
    d["key"] = original
    assert torch.equal(d["key"], original)

    d.clear()
