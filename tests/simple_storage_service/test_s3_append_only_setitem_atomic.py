"""Verify that BasicS3Dict append-only __setitem__ avoids __contains__.

After the refactoring to use setdefault_if for append-only writes,
BasicS3Dict.__setitem__ no longer performs a separate __contains__
(HEAD) check before writing.  The existence check is folded into the
setdefault_if → _actual_etag → etag path, and the write uses a
conditional PUT (IfNoneMatch: *) for atomic insert-if-absent.
"""

from moto import mock_aws
import pytest

from persidict import BasicS3Dict


@mock_aws
def test_append_only_setitem_skips_contains_on_insert():
    """__setitem__ on a fresh key must not call __contains__."""
    d = BasicS3Dict(
        bucket_name="ao-atomic-bucket",
        serialization_format="json",
        append_only=True,
    )

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        d["k"] = "v"
    finally:
        type(d).__contains__ = original_contains

    assert d["k"] == "v"
    assert contains_calls == 0, (
        f"__setitem__ performed {contains_calls} __contains__ call(s); "
        "expected 0 (existence check is via _actual_etag, not __contains__)")


@mock_aws
def test_append_only_setitem_skips_contains_on_duplicate():
    """__setitem__ rejecting a duplicate must not call __contains__."""
    d = BasicS3Dict(
        bucket_name="ao-atomic-dup-bucket",
        serialization_format="json",
        append_only=True,
    )
    d["k"] = "original"

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        with pytest.raises(KeyError):
            d["k"] = "replacement"
    finally:
        type(d).__contains__ = original_contains

    assert d["k"] == "original"
    assert contains_calls == 0, (
        f"__setitem__ performed {contains_calls} __contains__ call(s); "
        "expected 0 (existence check is via setdefault_if, not __contains__)")
