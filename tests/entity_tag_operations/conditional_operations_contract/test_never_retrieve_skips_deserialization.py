"""Tests that NEVER_RETRIEVE does not deserialize the stored value.

When retrieve_value=NEVER_RETRIEVE is used, the code path should only
inspect metadata (ETag/stat) and never attempt to read or parse the
underlying file contents. This is verified by corrupting the file after
writing a valid value and confirming that get_item_if succeeds without
raising a deserialization error.
"""

import pytest
from moto import mock_aws

from persidict import FileDirDict
from persidict.jokers_and_status_flags import (
    NEVER_RETRIEVE,
    VALUE_NOT_RETRIEVED,
    ETAG_IS_THE_SAME,
    ANY_ETAG,
)

from tests.data_for_mutable_tests import mutable_tests, make_test_dict


def test_never_retrieve_skips_deserialization_on_corrupted_json(tmp_path):
    """NEVER_RETRIEVE returns VALUE_NOT_RETRIEVED even when file is corrupted."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json",
                    digest_len=0)
    d["key"] = {"valid": True}
    etag = d.etag("key")

    # Corrupt the underlying file with invalid JSON
    data_file = tmp_path / "key.json"
    data_file.write_text("<<<NOT VALID JSON>>>")

    result = d.get_item_if(
        "key",
        condition=ETAG_IS_THE_SAME,
        expected_etag=etag,
        retrieve_value=NEVER_RETRIEVE,
    )

    assert result.new_value is VALUE_NOT_RETRIEVED
    assert isinstance(result.resulting_etag, str)


def test_never_retrieve_skips_deserialization_on_corrupted_pkl(tmp_path):
    """NEVER_RETRIEVE returns VALUE_NOT_RETRIEVED even when pkl file is corrupted."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="pkl",
                    digest_len=0)
    d["key"] = [1, 2, 3]
    etag = d.etag("key")

    # Corrupt the underlying file with invalid pickle data
    data_file = tmp_path / "key.pkl"
    data_file.write_bytes(b"\x00\x01CORRUPT_PICKLE_DATA\xff")

    result = d.get_item_if(
        "key",
        condition=ANY_ETAG,
        expected_etag=etag,
        retrieve_value=NEVER_RETRIEVE,
    )

    assert result.new_value is VALUE_NOT_RETRIEVED
    assert isinstance(result.resulting_etag, str)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_never_retrieve_returns_value_not_retrieved_all_backends(
        tmpdir, DictToTest, kwargs):
    """NEVER_RETRIEVE consistently returns VALUE_NOT_RETRIEVED across backends."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "hello"
    etag = d.etag("k")

    result = d.get_item_if(
        "k",
        condition=ETAG_IS_THE_SAME,
        expected_etag=etag,
        retrieve_value=NEVER_RETRIEVE,
    )

    assert result.condition_was_satisfied
    assert result.new_value is VALUE_NOT_RETRIEVED
    assert result.resulting_etag == etag
