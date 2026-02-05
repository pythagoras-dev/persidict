import os

from persidict import FileDirDict


def _set_mtime_ns(path: str, mtime_ns: int) -> None:
    try:
        os.utime(path, ns=(mtime_ns, mtime_ns))
    except (AttributeError, TypeError, ValueError):
        os.utime(path, (mtime_ns / 1_000_000_000, mtime_ns / 1_000_000_000))


def test_filedirdict_etag_uses_stat_components(tmpdir):
    d = FileDirDict(base_dir=tmpdir, serialization_format="json")
    key = "key1"
    d[key] = "value1"

    path = d._build_full_path(key)
    stat_result = os.stat(path)
    etag = d.etag(key)

    mtime_ns = getattr(stat_result, "st_mtime_ns", None)
    if mtime_ns is None:
        expected = f"{stat_result.st_mtime:.6f}:{stat_result.st_size}"
    else:
        expected = f"{mtime_ns}:{stat_result.st_size}"

    assert etag == expected


def test_filedirdict_etag_changes_with_size_when_mtime_fixed(tmpdir):
    d = FileDirDict(base_dir=tmpdir, serialization_format="json")
    key = "key2"
    d[key] = "a"

    path = d._build_full_path(key)
    stat_result = os.stat(path)
    mtime_ns = getattr(stat_result, "st_mtime_ns", None)
    if mtime_ns is None:
        fixed_time = stat_result.st_mtime
        os.utime(path, (fixed_time, fixed_time))
    else:
        _set_mtime_ns(path, mtime_ns)

    etag1 = d.etag(key)

    d[key] = "bb"
    if mtime_ns is None:
        os.utime(path, (fixed_time, fixed_time))
    else:
        _set_mtime_ns(path, mtime_ns)

    etag2 = d.etag(key)

    assert etag1 != etag2
