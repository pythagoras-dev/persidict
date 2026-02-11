"""Tests that FileDirDict public operations recover from transient PermissionError.

On Windows, concurrent file access causes transient PermissionError when
another process holds a lock.  FileDirDict retries these operations with
exponential backoff.  These tests verify that the public API is resilient:
each operation succeeds after a configurable number of transient failures
injected at the OS boundary.
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
    """__getitem__ succeeds after transient PermissionError on os.path.isfile."""
    d, path = _populated_dict(tmp_path)
    real_isfile = os.path.isfile
    monkeypatch.setattr(os.path, "isfile", _make_transient(real_isfile, 2))

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
    d, _ = _populated_dict(tmp_path)

    def always_fail(*args, **kwargs):
        raise PermissionError("permanent lock")

    monkeypatch.setattr(os.path, "isfile", always_fail)

    with pytest.raises(PermissionError):
        _ = d["k"]
