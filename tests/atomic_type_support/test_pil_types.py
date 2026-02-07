"""Tests for storing and retrieving PIL Image atomic types in PersiDict."""

import pytest
from PIL import Image

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pil_image_rgb(tmp_path, DictToTest):
    """Verify PIL RGB Image values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = Image.new("RGB", (10, 10), color="red")
    d["key"] = original
    retrieved = d["key"]
    assert list(retrieved.tobytes()) == list(original.tobytes())

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pil_image_grayscale(tmp_path, DictToTest):
    """Verify PIL grayscale Image values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = Image.new("L", (10, 10), color=128)
    d["key"] = original
    retrieved = d["key"]
    assert list(retrieved.tobytes()) == list(original.tobytes())

    d.clear()
