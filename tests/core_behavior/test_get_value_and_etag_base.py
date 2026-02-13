"""Tests for the base PersiDict._get_value_and_etag read-with-validation.

Every concrete subclass overrides _get_value_and_etag with a backend-specific
implementation, so this file exercises the *base class* logic through a
minimal stub that delegates to self[key] and self.etag(key).

Covers:
- Consistent value/etag return when nothing changes.
- Retry when the etag shifts between the pre-read and post-read checks.
- Fallback to last-read value after retries are exhausted.
- ValueError when _max_retries < 1.
"""

import pytest

from persidict import LocalDict
from persidict.persi_dict import PersiDict
from persidict.safe_str_tuple import NonEmptySafeStrTuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stub():
    """Return a LocalDict and forcibly unbind its _get_value_and_etag override.

    This makes LocalDict fall back to the base PersiDict implementation,
    which uses self.etag() / self[key] / self.etag() in a loop.
    """
    d = LocalDict(serialization_format="json")
    # Remove the subclass override so the base class method is used.
    d._get_value_and_etag = PersiDict._get_value_and_etag.__get__(d, type(d))
    return d


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_consistent_return_without_modification():
    """Base _get_value_and_etag returns matching value and etag."""
    d = _make_stub()
    d["k"] = "hello"
    expected_etag = d.etag("k")

    key = NonEmptySafeStrTuple("k")
    value, etag = d._get_value_and_etag(key)

    assert value == "hello"
    assert etag == expected_etag


def test_retry_on_mid_read_etag_change(monkeypatch):
    """If the etag changes between pre-read and post-read, the method retries.

    Simulates a single mid-read change: the first etag() call returns a stale
    value, causing a mismatch with the post-read etag. The second attempt
    succeeds because both etag calls return the same value.
    """
    d = _make_stub()
    d["k"] = "value"

    real_etag = d.etag
    call_count = 0

    def etag_with_one_stale_call(key):
        nonlocal call_count
        call_count += 1
        result = real_etag(key)
        if call_count == 1:
            return result + "_stale"
        return result

    monkeypatch.setattr(d, "etag", etag_with_one_stale_call)

    key = NonEmptySafeStrTuple("k")
    value, etag = d._get_value_and_etag(key)

    assert value == "value"
    # Attempt 1: etag_before (stale, count=1) != etag_after (real, count=2) → retry
    # Attempt 2: etag_before (real, count=3) == etag_after (real, count=4) → success
    assert call_count == 4


def test_fallback_after_retries_exhausted(monkeypatch):
    """When every retry sees a different etag, falls back to post-read etag.

    Even in the fallback case the method returns a value and a valid etag.
    """
    d = _make_stub()
    d["k"] = "data"

    real_etag = d.etag
    counter = 0

    def always_changing_etag(key):
        nonlocal counter
        counter += 1
        return real_etag(key) + f"_v{counter}"

    monkeypatch.setattr(d, "etag", always_changing_etag)

    key = NonEmptySafeStrTuple("k")
    value, etag = d._get_value_and_etag(key)

    assert value == "data"
    assert isinstance(etag, str)
    # 3 retries × 2 etag calls each = 6
    assert counter == 6


def test_max_retries_less_than_one_raises_value_error():
    """_max_retries < 1 raises ValueError."""
    d = _make_stub()
    d["k"] = "v"

    key = NonEmptySafeStrTuple("k")
    with pytest.raises(ValueError):
        d._get_value_and_etag(key, _max_retries=0)


def test_missing_key_raises_key_error():
    """Requesting a nonexistent key raises KeyError."""
    d = _make_stub()

    key = NonEmptySafeStrTuple("nonexistent")
    with pytest.raises(KeyError):
        d._get_value_and_etag(key)
