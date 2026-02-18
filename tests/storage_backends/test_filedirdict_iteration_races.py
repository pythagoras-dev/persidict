"""Tests for FileDirDict iteration resilience under concurrent modification.

Verifies that items()/values() skip entries whose underlying file disappears
between the directory listing and the file read, and that
items_and_timestamps() returns timestamps consistent with the values read.
"""

import os
import time

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


def test_items_and_timestamps_value_timestamp_consistency(tmp_path):
    """items_and_timestamps() returns timestamps matching the values read.

    Writes a value, overwrites it with a new value after a delay, and
    verifies that the timestamp returned alongside each value corresponds
    to the version of the file that was actually read (not a stale or
    future timestamp from a different version).
    """
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json",
                    digest_len=0)
    d["a"] = "first"
    ts_first = d.timestamp("a")

    # Ensure the second write gets a different mtime
    time.sleep(0.05)
    d["a"] = "second"
    ts_second = d.timestamp("a")
    assert ts_second > ts_first

    results = list(d.items_and_timestamps())
    assert len(results) == 1
    key, value, ts = results[0]

    # The value and timestamp must describe the same file version.
    assert value == "second"
    assert ts == ts_second
