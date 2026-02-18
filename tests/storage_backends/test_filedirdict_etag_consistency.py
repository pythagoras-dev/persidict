"""Tests for FileDirDict fstat-based etag/value consistency.

Verifies that _get_value_and_etag (exercised via get_item_if) uses
os.fstat on the open file descriptor so the returned etag always
corresponds to the returned value, and that the double-fstat guard
detects in-place modifications.
"""

import os

import pytest

from persidict import FileDirDict
from persidict.file_dir_dict import _InPlaceModificationError
from persidict.jokers_and_status_flags import (
    ALWAYS_RETRIEVE,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
)


def _make_dict(tmp_path):
    return FileDirDict(base_dir=str(tmp_path), serialization_format="json")


def _fake_stat_result(real, *, mtime_offset=0):
    """Build an os.stat_result with a shifted st_mtime."""
    return os.stat_result((
        real.st_mode, real.st_ino, real.st_dev,
        real.st_nlink, real.st_uid, real.st_gid,
        real.st_size,
        real.st_atime, real.st_mtime + mtime_offset, real.st_ctime,
    ))


def test_consistent_etag_when_file_unchanged(tmp_path):
    """get_item_if returns matching value and etag for an unmodified file."""
    d = _make_dict(tmp_path)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.new_value == "hello"
    assert result.actual_etag == etag


def test_fstat_guard_retries_on_inplace_modification(tmp_path, monkeypatch):
    """Double-fstat guard detects in-place modification and retries.

    Simulates an external process modifying the file in-place by making
    the second os.fstat call (the post-read check) return a different
    mtime on the first attempt. The retry via _with_retry should
    converge and return a consistent value/etag pair.
    """
    d = _make_dict(tmp_path)
    d["k"] = "value"
    etag = d.etag("k")

    real_fstat = os.fstat
    fstat_calls = 0

    def counting_fstat(fd):
        nonlocal fstat_calls
        result = real_fstat(fd)
        fstat_calls += 1
        # On the 2nd fstat call (post-read of first attempt),
        # return a shifted mtime to simulate in-place modification.
        if fstat_calls == 2:
            return _fake_stat_result(result, mtime_offset=1)
        return result

    monkeypatch.setattr(os, "fstat", counting_fstat)

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.new_value == "value"
    assert result.actual_etag == etag


def test_persistent_inplace_modification_raises(tmp_path, monkeypatch):
    """When fstat always detects in-place modification, retries are
    exhausted and _InPlaceModificationError propagates."""
    d = _make_dict(tmp_path)
    d["k"] = "data"

    real_fstat = os.fstat
    counter = 0

    def always_changing_fstat(fd):
        nonlocal counter
        result = real_fstat(fd)
        counter += 1
        # Every post-read fstat (even calls) returns a different mtime.
        if counter % 2 == 0:
            return _fake_stat_result(result, mtime_offset=counter)
        return result

    monkeypatch.setattr(os, "fstat", always_changing_fstat)

    with pytest.raises(_InPlaceModificationError):
        d._read_from_file(d._build_full_path("k"))


def test_deleted_before_open_raises_key_error(tmp_path):
    """If the file does not exist at open time, KeyError is raised."""
    d = _make_dict(tmp_path)
    d["k"] = "ephemeral"
    etag_before = d.etag("k")
    os.remove(d._build_full_path("k"))

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag=etag_before,
        retrieve_value=ALWAYS_RETRIEVE,
    )
    # File does not exist; get_item_if catches the KeyError and
    # reports the item as not available.
    assert result.actual_etag is ITEM_NOT_AVAILABLE
