"""Tests for FileDirDict stat-read-stat etag/value consistency.

Verifies that _get_value_and_etag (exercised via get_item_if) retries
when the file is modified mid-read, so the returned etag always
corresponds to the returned value.
"""

import os

from persidict import FileDirDict
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


def test_retry_on_concurrent_modification(tmp_path, monkeypatch):
    """Stat-read-stat detects a mid-read change and retries.

    Simulates a file modified between the first stat and the second stat
    by returning a stale mtime on the very first os.stat call. The retry
    should converge and return a consistent value/etag pair.
    """
    d = _make_dict(tmp_path)
    d["k"] = "value"
    path = d._build_full_path("k")
    etag = d.etag("k")

    real_stat = os.stat
    call_count = 0

    def counting_stat(p, *args, **kwargs):
        nonlocal call_count
        result = real_stat(p, *args, **kwargs)
        if p == path:
            call_count += 1
            if call_count == 1:
                return _fake_stat_result(result, mtime_offset=-1)
        return result

    monkeypatch.setattr(os, "stat", counting_stat)

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.new_value == "value"
    # First attempt: stat_before (stale, count=1) != stat_after (real, count=2)
    # → retry. Second attempt: stat_before (count=3) == stat_after (count=4).
    assert call_count == 4


def test_fallback_after_retries_exhausted(tmp_path, monkeypatch):
    """When every retry sees a different stat, falls back to post-read etag.

    Even in the fallback case the method must return a value and a valid
    etag (not raise).
    """
    d = _make_dict(tmp_path)
    d["k"] = "data"
    path = d._build_full_path("k")

    real_stat = os.stat
    counter = 0

    def always_changing_stat(p, *args, **kwargs):
        nonlocal counter
        result = real_stat(p, *args, **kwargs)
        if p == path:
            counter += 1
            return _fake_stat_result(result, mtime_offset=counter)
        return result

    monkeypatch.setattr(os, "stat", always_changing_stat)

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag="will-not-match",
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.new_value == "data"
    assert result.actual_etag is not None
    # All 3 retries attempted, 2 stat calls each = 6
    assert counter == 6


def test_deleted_during_read_raises_key_error(tmp_path, monkeypatch):
    """If the file vanishes between stat_before and stat_after, KeyError."""
    d = _make_dict(tmp_path)
    d["k"] = "ephemeral"
    path = d._build_full_path("k")
    etag_before = d.etag("k")

    real_stat = os.stat
    call_count = 0

    def stat_then_vanish(p, *args, **kwargs):
        nonlocal call_count
        if p == path:
            call_count += 1
            if call_count >= 2:
                raise FileNotFoundError(f"File {p} does not exist")
        return real_stat(p, *args, **kwargs)

    monkeypatch.setattr(os, "stat", stat_then_vanish)

    result = d.get_item_if(
        "k", condition=ETAG_IS_THE_SAME, expected_etag=etag_before,
        retrieve_value=ALWAYS_RETRIEVE,
    )
    # File vanished mid-read; get_item_if catches the KeyError and
    # reports the item as not available.
    assert result.actual_etag is ITEM_NOT_AVAILABLE
