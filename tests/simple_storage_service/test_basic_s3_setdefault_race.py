"""Tests for BasicS3Dict.setdefault and setdefault_if race condition handling.

Exercises the retry logic in setdefault() and the KeyError fallback in
setdefault_if() that protect against concurrent deletion between a failed
conditional put and the subsequent read.
"""

import pytest
from unittest.mock import patch, PropertyMock
from botocore.exceptions import ClientError
from moto import mock_aws

from persidict import BasicS3Dict
from persidict.basic_s3_dict import _MAX_SETDEFAULT_RETRIES
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
)


def _make_conditional_client_error():
    """Build a ClientError that looks like a failed S3 conditional put."""
    return ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 412},
         "Error": {"Code": "PreconditionFailed"}},
        "PutObject",
    )


@mock_aws
def test_setdefault_retries_on_concurrent_delete():
    """setdefault recovers when a key is deleted between put failure and read.

    Simulates: put(IfNoneMatch=*) fails → key deleted → retry succeeds.
    """
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")

    call_count = 0
    original_put = d.s3_client.put_object

    def put_then_succeed(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First attempt: pretend key exists (conditional failure)
            raise _make_conditional_client_error()
        # Second attempt: key is now absent, put succeeds
        return original_put(**kwargs)

    with patch.object(d.s3_client, "put_object", side_effect=put_then_succeed):
        result = d.setdefault("k", "default_val")

    assert result == "default_val"
    assert d["k"] == "default_val"
    assert call_count == 2


@mock_aws
def test_setdefault_returns_existing_after_conditional_failure():
    """setdefault returns existing value when put fails and key still exists."""
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")
    d["k"] = "existing"

    result = d.setdefault("k", "new_val")

    assert result == "existing"
    assert d["k"] == "existing"


@mock_aws
def test_setdefault_exhausts_retries_raises_runtime_error():
    """setdefault raises RuntimeError after exhausting all retries.

    Every attempt sees conditional failure + KeyError (perpetual race).
    """
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")

    def always_conflict(**kwargs):
        raise _make_conditional_client_error()

    with patch.object(d.s3_client, "put_object", side_effect=always_conflict):
        with pytest.raises(RuntimeError, match="retries"):
            d.setdefault("k", "val")


@mock_aws
def test_setdefault_propagates_non_conditional_client_error():
    """setdefault re-raises ClientError that is not a conditional failure."""
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")

    access_denied = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 403},
         "Error": {"Code": "AccessDenied"}},
        "PutObject",
    )

    with patch.object(d.s3_client, "put_object", side_effect=access_denied):
        with pytest.raises(ClientError):
            d.setdefault("k", "val")


@mock_aws
def test_setdefault_if_returns_item_not_available_on_concurrent_delete():
    """setdefault_if reports absent key when it vanishes during fallback read.

    Simulates: put(IfNoneMatch=*) fails → etag check sees key → read raises
    KeyError because key was deleted between the etag check and the read.
    The result should indicate the item ended up absent.
    """
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")

    etag_call_count = 0

    def racy_actual_etag(key):
        nonlocal etag_call_count
        etag_call_count += 1
        if etag_call_count == 1:
            # First call: key absent → proceed to put
            return ITEM_NOT_AVAILABLE
        # Second call (inside except): key appears briefly
        return "fake-etag-12345"

    def getitem_raises(self, key):
        raise KeyError(key)

    # Simulate: put fails with conditional error, etag returns a value,
    # but __getitem__ raises KeyError (concurrent delete between checks)
    with patch.object(d, "_actual_etag", side_effect=racy_actual_etag), \
         patch.object(d, "_put_object_with_conditions",
                      side_effect=_make_conditional_client_error()), \
         patch.object(type(d), "__getitem__", getitem_raises):
        result = d.setdefault_if(
            "k", "default_val", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert result.actual_etag is ITEM_NOT_AVAILABLE
    assert result.resulting_etag is ITEM_NOT_AVAILABLE
    assert result.new_value is ITEM_NOT_AVAILABLE


@mock_aws
def test_setdefault_if_returns_existing_when_key_persists():
    """setdefault_if returns existing value when concurrent insert wins."""
    d = BasicS3Dict(bucket_name="race-bucket", serialization_format="json")

    # Another writer inserts the key before us
    d["k"] = "winner"

    result = d.setdefault_if(
        "k", "default_val", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)

    assert not result.condition_was_satisfied
    assert result.new_value == "winner"


@mock_aws
def test_max_setdefault_retries_is_positive():
    """Verify the retry constant is a sensible positive integer."""
    assert isinstance(_MAX_SETDEFAULT_RETRIES, int)
    assert _MAX_SETDEFAULT_RETRIES >= 3
