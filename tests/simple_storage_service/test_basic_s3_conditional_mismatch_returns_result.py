"""Tests that BasicS3Dict returns a structured result on ETag mismatch.

When a conditional write fails because the ETag does not match, the operation
must return a ConditionalOperationResult with condition_was_satisfied=False
rather than raising a ClientError.
"""

from moto import mock_aws

from persidict import BasicS3Dict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ALWAYS_RETRIEVE,
)


@mock_aws
def test_set_item_if_mismatch_returns_result_not_exception():
    """ETag mismatch on set_item_if yields structured result, no exception."""
    d = BasicS3Dict(bucket_name="mismatch-bucket", serialization_format="json")
    d["k"] = "v1"
    old_etag = d.etag("k")

    # Update the key so the old ETag becomes stale
    d["k"] = "v2"
    current_etag = d.etag("k")
    assert current_etag != old_etag

    result = d.set_item_if(
        "k",
        value="v3",
        condition=ETAG_IS_THE_SAME,
        expected_etag=old_etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert not result.condition_was_satisfied
    assert result.actual_etag == current_etag
    assert result.resulting_etag == current_etag
    assert result.new_value == "v2"


@mock_aws
def test_discard_item_if_mismatch_returns_result_not_exception():
    """ETag mismatch on discard_item_if yields structured result, no exception."""
    d = BasicS3Dict(bucket_name="mismatch-bucket", serialization_format="json")
    d["k"] = "v1"
    old_etag = d.etag("k")

    d["k"] = "v2"

    result = d.discard_item_if(
        "k",
        condition=ETAG_IS_THE_SAME,
        expected_etag=old_etag,
    )

    assert not result.condition_was_satisfied
    assert "k" in d
    assert d["k"] == "v2"


@mock_aws
def test_set_item_if_matching_etag_succeeds():
    """set_item_if with correct ETag succeeds and returns new ETag."""
    d = BasicS3Dict(bucket_name="mismatch-bucket", serialization_format="json")
    d["k"] = "v1"
    etag = d.etag("k")

    result = d.set_item_if(
        "k",
        value="v2",
        condition=ETAG_IS_THE_SAME,
        expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.condition_was_satisfied
    assert result.new_value == "v2"
    assert isinstance(result.resulting_etag, str)
    assert result.resulting_etag != etag
    assert d["k"] == "v2"
