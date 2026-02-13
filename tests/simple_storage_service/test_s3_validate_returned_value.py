"""Tests for read-side base_class_for_values enforcement on BasicS3Dict.

BasicS3Dict has multiple read paths that must validate returned values:
__getitem__, _get_value_and_etag (via get_item_if with ALWAYS_RETRIEVE),
the inline get_item_if conditional path (IF_ETAG_CHANGED), and the
_generic_iter iterator used by values()/items().
"""

import pytest
from moto import mock_aws

from persidict import BasicS3Dict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME, ALWAYS_RETRIEVE, IF_ETAG_CHANGED,
)


def _make_constrained_pair(**s3_kwargs):
    """Create an unconstrained and int-constrained handle sharing one bucket."""
    unconstrained = BasicS3Dict(
        bucket_name="validate-bucket", serialization_format="pkl",
        **s3_kwargs,
    )
    constrained = BasicS3Dict(
        bucket_name="validate-bucket", serialization_format="pkl",
        base_class_for_values=int, **s3_kwargs,
    )
    return unconstrained, constrained


@mock_aws
def test_s3_getitem_rejects_mismatched_type():
    """__getitem__ raises TypeError when the stored value violates the constraint."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "not-an-int"

    with pytest.raises(TypeError, match="int"):
        _ = constrained["k"]


@mock_aws
def test_s3_getitem_accepts_matching_type():
    """__getitem__ returns the value when it satisfies base_class_for_values."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = 42

    assert constrained["k"] == 42


@mock_aws
def test_s3_get_item_if_always_retrieve_rejects_mismatched_type():
    """get_item_if with ALWAYS_RETRIEVE exercises _get_value_and_etag."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "not-an-int"
    etag = unconstrained.etag("k")

    with pytest.raises(TypeError, match="int"):
        constrained.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=etag,
            retrieve_value=ALWAYS_RETRIEVE)


@mock_aws
def test_s3_get_item_if_etag_changed_rejects_mismatched_type():
    """get_item_if with IF_ETAG_CHANGED exercises the inline S3 read path.

    Uses a stale etag so IfNoneMatch does not trigger a 304 and the value
    is actually fetched and deserialized.
    """
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "placeholder"
    stale_etag = unconstrained.etag("k")
    unconstrained["k"] = "not-an-int"

    with pytest.raises(TypeError, match="int"):
        constrained.get_item_if(
            "k", condition=ETAG_IS_THE_SAME, expected_etag=stale_etag,
            retrieve_value=IF_ETAG_CHANGED)


@mock_aws
def test_s3_values_rejects_mismatched_type():
    """Iterating values() through a constrained handle raises on mismatch."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "not-an-int"

    with pytest.raises(TypeError, match="int"):
        list(constrained.values())


@mock_aws
def test_s3_items_rejects_mismatched_type():
    """Iterating items() through a constrained handle raises on mismatch."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "not-an-int"

    with pytest.raises(TypeError, match="int"):
        list(constrained.items())


@mock_aws
def test_s3_keys_skips_validation():
    """Iterating keys() does not validate values, so no TypeError."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = "not-an-int"

    assert len(list(constrained.keys())) == 1


@mock_aws
def test_s3_accepts_subclass():
    """A value that is a subclass of base_class_for_values passes validation."""
    unconstrained, constrained = _make_constrained_pair()
    unconstrained["k"] = True  # bool is a subclass of int

    assert constrained["k"] is True
