"""Tests for the custom exception taxonomy defined in persidict.exceptions.

Verifies structured fields, inheritance hierarchy, KeyError argument invariants,
exception chaining, and that policy violations raise the correct exception type.
"""

import os
import pytest
from moto import mock_aws

from persidict import (
    MutationPolicyError,
    ConcurrencyConflictError,
    BackendError,
    FileDirDict,
    LocalDict,
    BasicS3Dict,
)
from persidict.safe_str_tuple import NonEmptySafeStrTuple
from tests.data_for_mutable_tests import make_test_dict


# -- Structured fields on custom exceptions ----------------------------------

@pytest.mark.parametrize("policy", ["append-only", "write-once"])
def test_mutation_policy_error_has_policy_field(policy):
    """MutationPolicyError stores the policy name as a structured field."""
    exc = MutationPolicyError(policy)

    assert exc.policy == policy
    assert isinstance(exc, TypeError)


def test_concurrency_conflict_error_has_key_and_attempts():
    """ConcurrencyConflictError stores key and attempts as structured fields."""
    exc = ConcurrencyConflictError(("a", "b"), 5)

    assert exc.key == ("a", "b")
    assert exc.attempts == 5
    assert isinstance(exc, RuntimeError)


def test_backend_error_has_backend_and_operation():
    """BackendError stores backend, operation, and key as structured fields."""
    exc = BackendError("boom", backend="filesystem", operation="init", key="k")

    assert exc.backend == "filesystem"
    assert exc.operation == "init"
    assert exc.key == "k"
    assert isinstance(exc, RuntimeError)


def test_backend_error_key_defaults_to_none():
    """BackendError.key defaults to None when not provided."""
    exc = BackendError("boom", backend="s3", operation="put_object")

    assert exc.key is None


# -- KeyError argument is the raw key ----------------------------------------

@pytest.mark.parametrize("DictClass, kwargs", [
    (FileDirDict, dict(serialization_format="json")),
    (LocalDict, dict(serialization_format="json", bucket_name="test_bucket")),
])
def test_getitem_missing_key_error_arg_is_safe_str_tuple(tmp_path, DictClass, kwargs):
    """KeyError.args[0] is a NonEmptySafeStrTuple, not a message string."""
    d = make_test_dict(DictClass, tmp_path, **kwargs)

    with pytest.raises(KeyError) as exc_info:
        d["nonexistent"]

    assert isinstance(exc_info.value.args[0], NonEmptySafeStrTuple)


@mock_aws
def test_s3_getitem_missing_key_error_arg_is_safe_str_tuple():
    """S3 backend: KeyError.args[0] is a NonEmptySafeStrTuple."""
    d = BasicS3Dict(serialization_format="json", bucket_name="test-bucket")

    with pytest.raises(KeyError) as exc_info:
        d["nonexistent"]

    assert isinstance(exc_info.value.args[0], NonEmptySafeStrTuple)


# -- Exception chaining -------------------------------------------------------

def test_filedirdict_getitem_chains_filenotfounderror(tmp_path):
    """FileDirDict.__getitem__ chains FileNotFoundError as __cause__."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")

    with pytest.raises(KeyError) as exc_info:
        d["missing"]

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


# -- MutationPolicyError for append-only violations ---------------------------

@pytest.mark.parametrize("DictClass, kwargs", [
    (FileDirDict, dict(serialization_format="json", append_only=True)),
    (LocalDict, dict(serialization_format="json", bucket_name="ao",
                     append_only=True)),
])
def test_append_only_overwrite_raises_mutation_policy_error(
        tmp_path, DictClass, kwargs):
    """Overwriting an existing key in append-only mode raises MutationPolicyError."""
    d = make_test_dict(DictClass, tmp_path, **kwargs)
    d["k"] = "original"

    with pytest.raises(MutationPolicyError) as exc_info:
        d["k"] = "replacement"

    assert exc_info.value.policy == "append-only"


@pytest.mark.parametrize("DictClass, kwargs", [
    (FileDirDict, dict(serialization_format="json", append_only=True)),
    (LocalDict, dict(serialization_format="json", bucket_name="ao",
                     append_only=True)),
])
def test_append_only_delete_raises_mutation_policy_error(
        tmp_path, DictClass, kwargs):
    """Deleting a key in append-only mode raises MutationPolicyError."""
    d = make_test_dict(DictClass, tmp_path, **kwargs)
    d["k"] = "value"

    with pytest.raises(MutationPolicyError) as exc_info:
        del d["k"]

    assert exc_info.value.policy == "append-only"


# -- BackendError for infrastructure failure -----------------------------------

def test_filedirdict_init_unreachable_dir_raises_backend_error(tmp_path, monkeypatch):
    """FileDirDict raises BackendError when base_dir exists but is not a directory."""
    target = str(tmp_path / "ghost")

    monkeypatch.setattr(os, "makedirs", lambda *a, **kw: None)
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    monkeypatch.setattr(os.path, "isdir", lambda p: False)

    with pytest.raises(BackendError) as exc_info:
        FileDirDict(base_dir=target, serialization_format="json")

    assert exc_info.value.backend == "filesystem"
    assert exc_info.value.operation == "init"
