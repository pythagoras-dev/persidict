"""Tests for FileDirDict retry and error-handling behavior at the OS boundary.

Covers two concerns:
- Transient PermissionError recovery: On Windows, concurrent file access
  causes transient PermissionError.  FileDirDict retries with exponential
  backoff, and these tests verify that public operations succeed after a
  configurable number of transient failures.
- FileNotFoundError fast-fail: When a file is genuinely missing,
  _read_from_file raises immediately (no retries), and __getitem__
  converts the error to KeyError.
"""

import os

import pytest

from persidict import FileDirDict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_transient(real_fn, n_failures):
    """Return a wrapper that raises PermissionError *n_failures* times, then delegates."""
    call_count = 0

    def wrapper(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= n_failures:
            raise PermissionError("transient lock")
        return real_fn(*args, **kwargs)

    wrapper.call_count = lambda: call_count
    return wrapper


def _populated_dict(tmp_path):
    """Return a FileDirDict with one entry and its resolved file path."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "hello"
    path = d._build_full_path("k")
    return d, path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_getitem_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """__getitem__ succeeds after transient PermissionError on open()."""
    import builtins
    d, path = _populated_dict(tmp_path)
    real_open = builtins.open
    monkeypatch.setattr(builtins, "open", _make_transient(real_open, 2))

    assert d["k"] == "hello"


def test_contains_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """__contains__ succeeds after transient PermissionError on os.path.isfile."""
    d, _ = _populated_dict(tmp_path)
    real_isfile = os.path.isfile
    monkeypatch.setattr(os.path, "isfile", _make_transient(real_isfile, 2))

    assert "k" in d


def test_delitem_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """__delitem__ succeeds after transient PermissionError on os.remove."""
    d, path = _populated_dict(tmp_path)
    real_remove = os.remove
    monkeypatch.setattr(os, "remove", _make_transient(real_remove, 2))

    del d["k"]
    assert "k" not in d


def test_etag_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """etag() succeeds after transient PermissionError on os.stat."""
    d, _ = _populated_dict(tmp_path)
    real_stat = os.stat
    monkeypatch.setattr(os, "stat", _make_transient(real_stat, 2))

    etag = d.etag("k")
    assert isinstance(etag, str) and len(etag) > 0


def test_timestamp_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """timestamp() succeeds after transient PermissionError on os.path.getmtime."""
    d, _ = _populated_dict(tmp_path)
    real_getmtime = os.path.getmtime
    monkeypatch.setattr(os.path, "getmtime", _make_transient(real_getmtime, 2))

    ts = d.timestamp("k")
    assert isinstance(ts, float) and ts > 0


def test_setdefault_recovers_from_transient_permission_error(tmp_path, monkeypatch):
    """setdefault() succeeds after transient PermissionError on os.path.isfile."""
    d, _ = _populated_dict(tmp_path)
    real_isfile = os.path.isfile
    monkeypatch.setattr(os.path, "isfile", _make_transient(real_isfile, 2))

    result = d.setdefault("k", "fallback")
    assert result == "hello"


def test_persistent_permission_error_is_raised(tmp_path, monkeypatch):
    """PermissionError is raised when all retries are exhausted."""
    import builtins
    d, _ = _populated_dict(tmp_path)

    def always_fail(*args, **kwargs):
        raise PermissionError("permanent lock")

    monkeypatch.setattr(builtins, "open", always_fail)

    with pytest.raises(PermissionError):
        _ = d["k"]


def test_getitem_missing_key_raises_key_error(tmp_path):
    """__getitem__ raises KeyError (not FileNotFoundError) for a missing key."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")

    with pytest.raises(KeyError):
        _ = d["nonexistent"]


def test_file_not_found_is_not_retried(tmp_path, monkeypatch):
    """FileNotFoundError in _read_from_file raises immediately without retries."""
    import builtins
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")

    call_count = 0
    def counting_open(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise FileNotFoundError("gone")

    monkeypatch.setattr(builtins, "open", counting_open)

    with pytest.raises(KeyError):
        _ = d["k"]

    assert call_count == 1
