"""Verify that all deletion operations raise MutationPolicyError on append-only dicts.

Deletion is an unsupported operation for append-only dictionaries, regardless
of the backend.  Every deletion path (__delitem__, clear, discard, pop,
popitem, discard_item_if) must raise MutationPolicyError consistently.
"""

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, MutationPolicyError, S3Dict_FileDirCached
from tests.data_for_mutable_tests import make_test_dict
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
def test_delitem_raises_mutation_policy_error(tmpdir, DictToTest, kwargs):
    """__delitem__ on an append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(MutationPolicyError):
        del d["k"]
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_clear_raises_mutation_policy_error(tmpdir, DictToTest, kwargs):
    """clear() on an append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(MutationPolicyError):
        d.clear()
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_discard_raises_mutation_policy_error(tmpdir, DictToTest, kwargs):
    """discard() on an append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(MutationPolicyError):
        d.discard("k")
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_pop_raises_mutation_policy_error(tmpdir, DictToTest, kwargs):
    """pop() on an append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(MutationPolicyError):
        d.pop("k")
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_popitem_raises_mutation_policy_error(tmpdir, DictToTest, kwargs):
    """popitem() on an append-only dict raises MutationPolicyError."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    with pytest.raises(MutationPolicyError):
        d.popitem()
    assert "k" in d


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@pytest.mark.parametrize("condition", [ANY_ETAG, ETAG_IS_THE_SAME, ETAG_HAS_CHANGED])
@mock_aws
def test_discard_item_if_raises_mutation_policy_error(tmpdir, DictToTest, kwargs, condition):
    """discard_item_if() on an append-only dict raises MutationPolicyError
    when the condition would be satisfied."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "v"
    etag = d.etag("k")

    expected_etag_map = {
        ANY_ETAG: ITEM_NOT_AVAILABLE,
        ETAG_IS_THE_SAME: etag,
        ETAG_HAS_CHANGED: "bogus-etag",
    }

    with pytest.raises(MutationPolicyError):
        d.discard_item_if("k", condition=condition, expected_etag=expected_etag_map[condition])
    assert "k" in d
