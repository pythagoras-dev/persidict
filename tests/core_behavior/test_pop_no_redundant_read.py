"""Verify that pop does not perform a redundant existence check.

pop delegates to transform_item, which reads the value via get_item_if
and deletes via discard_item_if → _remove_item.  The deletion path must
not re-check existence through __contains__, which would be a wasted
backend read.  This test guards against regressions.
"""

from persidict import FileDirDict, LocalDict


def test_pop_no_redundant_contains_file_dir(tmp_path):
    """FileDirDict.pop must not call __contains__ during delete."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "value"

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        result = d.pop("k")
    finally:
        type(d).__contains__ = original_contains

    assert result == "value"
    assert "k" not in d
    assert contains_calls == 0, (
        f"pop performed {contains_calls} __contains__ call(s); "
        "expected 0 (_remove_item should not re-check existence)")


def test_pop_no_redundant_contains_local(tmp_path):
    """LocalDict.pop must not call __contains__ during delete."""
    d = LocalDict(serialization_format="json")
    d["k"] = "value"

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        result = d.pop("k")
    finally:
        type(d).__contains__ = original_contains

    assert result == "value"
    assert "k" not in d
    assert contains_calls == 0, (
        f"pop performed {contains_calls} __contains__ call(s); "
        "expected 0 (_remove_item should not re-check existence)")


def test_pop_missing_key_with_default(tmp_path):
    """pop with default on missing key must not raise."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    result = d.pop("nonexistent", "fallback")
    assert result == "fallback"


def test_pop_missing_key_raises(tmp_path):
    """pop without default on missing key must raise KeyError."""
    import pytest
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    with pytest.raises(KeyError):
        d.pop("nonexistent")
