"""Verify that discard_item_if does not perform a redundant existence check.

discard_item_if already reads the backend once (via _actual_etag) to
determine whether the key is present.  The subsequent deletion must not
re-check existence through __contains__, which would be a wasted backend
read.  This test guards against regressions.
"""

from persidict import FileDirDict, LocalDict


def test_discard_item_if_no_redundant_contains_file_dir(tmp_path):
    """FileDirDict.discard_item_if must not call __contains__ during delete."""
    from persidict.jokers_and_status_flags import ETAG_IS_THE_SAME

    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k"] = "value"
    etag = d.etag("k")

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        result = d.discard_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    finally:
        type(d).__contains__ = original_contains

    assert result.condition_was_satisfied
    assert "k" not in d
    assert contains_calls == 0, (
        f"discard_item_if performed {contains_calls} __contains__ call(s); "
        "expected 0 (existence is already known from _actual_etag)")


def test_discard_item_if_no_redundant_contains_local(tmp_path):
    """LocalDict.discard_item_if must not call __contains__ during delete."""
    from persidict.jokers_and_status_flags import ETAG_IS_THE_SAME

    d = LocalDict(serialization_format="json")
    d["k"] = "value"
    etag = d.etag("k")

    contains_calls = 0
    original_contains = type(d).__contains__

    def counting_contains(self, key):
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(self, key)

    type(d).__contains__ = counting_contains
    try:
        result = d.discard_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    finally:
        type(d).__contains__ = original_contains

    assert result.condition_was_satisfied
    assert "k" not in d
    assert contains_calls == 0, (
        f"discard_item_if performed {contains_calls} __contains__ call(s); "
        "expected 0 (existence is already known from _actual_etag)")
