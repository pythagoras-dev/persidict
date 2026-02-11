"""Tests that LocalDict ETags remain unique across instances sharing a backend.

When multiple LocalDict instances (or a parent and its subdict) share the
same _RAMBackend, writes from any instance must produce globally unique ETags.
This file exercises the contract that the monotonic write counter lives on
the backend and is shared, not per-LocalDict-instance.
"""


from persidict import LocalDict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
)


def test_subdict_write_changes_parent_etag():
    """Writing via a subdict must produce a different ETag than the parent's write."""
    parent = LocalDict()
    parent["a", "b"] = 10
    etag_before = parent.etag(("a", "b"))

    sub = parent.get_subdict(["a"])
    sub["b"] = 20

    etag_after = parent.etag(("a", "b"))
    assert etag_before != etag_after
    assert parent["a", "b"] == 20


def test_parent_write_changes_subdict_etag():
    """Writing via the parent must produce a different ETag visible from the subdict."""
    parent = LocalDict()
    parent["a", "b"] = 10
    sub = parent.get_subdict(["a"])
    etag_before = sub.etag("b")

    parent["a", "b"] = 20
    etag_after = sub.etag("b")

    assert etag_before != etag_after
    assert sub["b"] == 20


def test_two_subdicts_produce_distinct_etags():
    """Two subdicts sharing a backend must not generate colliding ETags."""
    parent = LocalDict()
    parent["a", "x"] = 1
    parent["b", "x"] = 2

    sub_a = parent.get_subdict(["a"])
    sub_b = parent.get_subdict(["b"])

    sub_a["x"] = 10
    etag_a = sub_a.etag("x")
    sub_b["x"] = 20
    etag_b = sub_b.etag("x")

    assert etag_a != etag_b


def test_etag_monotonically_increases_across_instances():
    """ETags (as ints) must strictly increase regardless of which instance writes."""
    parent = LocalDict()
    sub = parent.get_subdict(["s"])
    etags = []

    parent["k"] = "v1"
    etags.append(int(parent.etag("k")))

    sub["j"] = "v2"
    etags.append(int(sub.etag("j")))

    parent["k"] = "v3"
    etags.append(int(parent.etag("k")))

    sub["j"] = "v4"
    etags.append(int(sub.etag("j")))

    assert etags == sorted(etags)
    assert len(set(etags)) == len(etags)


def test_conditional_set_detects_subdict_write():
    """A conditional set_item_if on the parent must detect a concurrent subdict write."""
    parent = LocalDict()
    parent["a", "b"] = "original"
    stale_etag = parent.etag(("a", "b"))

    sub = parent.get_subdict(["a"])
    sub["b"] = "sneaky_update"

    result = parent.set_item_if(("a", "b"), "should_fail", stale_etag, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert parent["a", "b"] == "sneaky_update"


def test_conditional_get_detects_subdict_write():
    """A conditional get_item_if on the parent must detect a concurrent subdict write."""
    parent = LocalDict()
    parent["a", "b"] = "v1"
    stale_etag = parent.etag(("a", "b"))

    sub = parent.get_subdict(["a"])
    sub["b"] = "v2"

    result = parent.get_item_if(("a", "b"), stale_etag, ETAG_HAS_CHANGED)

    assert result.condition_was_satisfied
    assert result.new_value == "v2"


def test_nested_subdicts_share_counter():
    """A subdict of a subdict must still share the same write counter."""
    root = LocalDict()
    root["a", "b", "c"] = "val"
    etag1 = root.etag(("a", "b", "c"))

    sub_ab = root.get_subdict(["a", "b"])
    sub_ab["c"] = "val2"
    etag2 = root.etag(("a", "b", "c"))

    assert etag1 != etag2
