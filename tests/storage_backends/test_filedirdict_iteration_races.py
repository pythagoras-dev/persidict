"""Tests for FileDirDict iteration resilience when keys are deleted mid-iteration.

Verifies that items()/values() skip entries whose underlying file disappears
between the directory listing and the file read, rather than crashing.
"""

import os

from persidict import FileDirDict


def test_items_skips_key_deleted_during_iteration(tmp_path):
    """items() silently skips a key whose file is deleted mid-iteration."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json",
                    digest_len=0)
    d["a"] = "alpha"
    d["b"] = "bravo"

    original_read = d._read_from_file
    deleted = []

    def delete_then_read(full_path):
        if full_path.endswith("b.json") and "b.json" not in deleted:
            deleted.append("b.json")
            os.remove(full_path)
            return original_read(full_path)
        return original_read(full_path)

    d._read_from_file = delete_then_read

    collected = dict(d.items())

    assert ("a",) in collected
    assert collected[("a",)] == "alpha"
    assert ("b",) not in collected


def test_values_skips_key_deleted_during_iteration(tmp_path):
    """values() completes without error when a file vanishes mid-iteration."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json",
                    digest_len=0)
    d["x"] = 1
    d["y"] = 2

    original_read = d._read_from_file
    deleted = []

    def delete_then_read(full_path):
        if full_path.endswith("x.json") and "x.json" not in deleted:
            deleted.append("x.json")
            os.remove(full_path)
            return original_read(full_path)
        return original_read(full_path)

    d._read_from_file = delete_then_read

    values = list(d.values())

    assert len(values) == 1
    assert 2 in values
