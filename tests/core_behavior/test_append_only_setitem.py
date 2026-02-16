"""Verify append-only __setitem__ contract across all backends.

Append-only dicts allow inserting a new key but reject overwrites of
existing keys with KeyError.  These tests exercise the __setitem__ path
directly (not conditional ops) so that every backend's insert-if-absent
routing is covered.
"""

import pytest
from moto import mock_aws

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached
from persidict import MutationPolicyError
from tests.data_for_mutable_tests import make_test_dict

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
def test_insert_new_key_succeeds(tmpdir, DictToTest, kwargs):
    """Inserting a fresh key into an append-only dict stores the value."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    d["k"] = "v"

    assert "k" in d
    assert d["k"] == "v"


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_overwrite_existing_key_raises(tmpdir, DictToTest, kwargs):
    """Overwriting an existing key raises MutationPolicyError and preserves the original."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)
    d["k"] = "original"

    with pytest.raises(MutationPolicyError):
        d["k"] = "replacement"

    assert d["k"] == "original"


@pytest.mark.parametrize("DictToTest, kwargs", append_only_tests)
@mock_aws
def test_multiple_distinct_keys_succeed(tmpdir, DictToTest, kwargs):
    """Multiple inserts with distinct keys all succeed."""
    d = make_test_dict(DictToTest, tmpdir, **kwargs)

    d["a"] = 1
    d["b"] = 2
    d["c"] = 3

    assert d["a"] == 1
    assert d["b"] == 2
    assert d["c"] == 3
    assert len(d) == 3
