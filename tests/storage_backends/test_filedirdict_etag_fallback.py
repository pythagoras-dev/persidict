"""Tests for FileDirDict ETag composition from stat components.

Verifies that the ETag incorporates mtime, file size, and inode so that
changes to any one of those fields produce a distinct ETag.
"""

import os
import tempfile

from persidict import FileDirDict


def _set_mtime_ns(path: str, mtime_ns: int) -> None:
    try:
        os.utime(path, ns=(mtime_ns, mtime_ns))
    except (AttributeError, TypeError, ValueError):
        os.utime(path, (mtime_ns / 1_000_000_000, mtime_ns / 1_000_000_000))


def test_etag_incorporates_mtime_size_and_inode(tmp_path):
    """ETag changes when any stat component (mtime, size, inode) differs."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "value1"

    path = d._build_full_path("k")
    stat_result = os.stat(path)
    etag = d.etag("k")

    # Sanity: the ETag is a non-empty string derived from stat fields.
    assert isinstance(etag, str) and len(etag) > 0

    # The ETag must contain all three stat components as substrings.
    mtime_ns = getattr(stat_result, "st_mtime_ns", None)
    mtime_part = str(mtime_ns) if mtime_ns is not None else f"{stat_result.st_mtime:.6f}"
    assert mtime_part in etag
    assert str(stat_result.st_size) in etag
    assert str(stat_result.st_ino) in etag


def test_etag_changes_with_size_when_mtime_fixed(tmp_path):
    """Writing a different-length value changes the ETag even when mtime is pinned."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "a"

    path = d._build_full_path("k")
    stat_result = os.stat(path)
    mtime_ns = getattr(stat_result, "st_mtime_ns", None)
    if mtime_ns is None:
        fixed_time = stat_result.st_mtime
        os.utime(path, (fixed_time, fixed_time))
    else:
        _set_mtime_ns(path, mtime_ns)
    etag1 = d.etag("k")

    d["k"] = "bb"
    if mtime_ns is None:
        os.utime(path, (fixed_time, fixed_time))
    else:
        _set_mtime_ns(path, mtime_ns)
    etag2 = d.etag("k")

    assert etag1 != etag2


def test_etag_changes_when_file_replaced_via_rename(tmp_path):
    """Replacing a file via atomic rename changes the ETag (new inode).

    Simulates an atomic-write pattern: write to a temp file, then
    os.replace into the original path.  Even if size and mtime happen
    to match, the new inode must produce a different ETag.
    """
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "original"

    path = d._build_full_path("k")
    stat_before = os.stat(path)
    etag_before = d.etag("k")

    # Read the original content, write it to a new temp file, then rename.
    with open(path, "rb") as f:
        content = f.read()

    dir_name = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=dir_name)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)

    # Pin mtime to match the original file.
    mtime_ns = getattr(stat_before, "st_mtime_ns", None)
    if mtime_ns is not None:
        os.utime(tmp, ns=(mtime_ns, mtime_ns))
    else:
        os.utime(tmp, (stat_before.st_mtime, stat_before.st_mtime))

    os.replace(tmp, path)

    stat_after = os.stat(path)
    etag_after = d.etag("k")

    # Same size and mtime, but a different inode.
    assert stat_after.st_size == stat_before.st_size
    assert stat_after.st_ino != stat_before.st_ino
    assert etag_after != etag_before
