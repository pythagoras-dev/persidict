"""Verify that all deletion operations raise TypeError on append-only dicts.

Deletion is an unsupported operation for append-only dictionaries, regardless
of the backend.  Every deletion path (__delitem__, clear, discard,
discard_item_if) must raise TypeError consistently.
"""

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached
from persidict.jokers_and_status_flags import (
    ANY_ETAG,
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
)

append_only_tests = [
    (FileDirDict, dict(serialization_format="json", append_only=True)),
    (LocalDict, dict(serialization_format="json", bucket_name="ao_bucket",
                     append_only=True)),
    (BasicS3Dict, dict(serialization_format="json", bucket_name="ao_bucket",
                       append_only=True)),
    (S3Dict_FileDirCached, dict(serialization_format="json",
                                bucket_name="ao_bucket", append_only=True)),
]


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_delitem_raises_type_error(tmpdir, DictToTest, kwargs):
    """__delitem__ on an append-only dict raises TypeError."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(TypeError):
        del d["k"]
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_clear_raises_type_error(tmpdir, DictToTest, kwargs):
    """clear() on an append-only dict raises TypeError."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(TypeError):
        d.clear()
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_discard_raises_type_error(tmpdir, DictToTest, kwargs):
    """discard() on an append-only dict raises TypeError."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(TypeError):
        d.discard("k")
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@pytest.mark.parametrize("condition", [ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED])
@mock_aws
def test_discard_item_if_raises_type_error(tmpdir, DictToTest, kwargs, condition):
    """discard_item_if() on an append-only dict raises TypeError
    when the condition would be satisfied."""
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d["k"] = "v"
    etag = d.etag("k")

    expected_etag_map = {
        ANY_ETAG: ITEM_NOT_AVAILABLE,
        ETAG_IS_THE_SAME: etag,
        ETAG_HAS_CHANGED: "bogus-etag",
    }

    with pytest.raises(TypeError):
        d.discard_item_if("k", expected_etag_map[condition], condition)
    assert "k" in d
