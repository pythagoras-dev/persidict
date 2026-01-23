"""Tests for __copy__ and __deepcopy__ methods on PersiDict implementations and SafeStrTuple.

These tests verify that copy semantics work correctly:
- Shallow copy creates a new instance pointing to the same underlying storage
- For immutable types (SafeStrTuple), copy returns the same object
"""

import copy
import pytest
from moto import mock_aws

from persidict import (
    FileDirDict,
    LocalDict,
    S3Dict_FileDirCached,
    SafeStrTuple,
    NonEmptySafeStrTuple,
)

from .data_for_mutable_tests import mutable_tests


# =============================================================================
# SafeStrTuple copy tests
# =============================================================================


def test_safe_str_tuple_copy_returns_same_object():
    """Immutable SafeStrTuple returns itself on copy since no copying is needed."""
    original = SafeStrTuple("a", "b", "c")
    copied = copy.copy(original)
    assert copied is original


def test_safe_str_tuple_deepcopy_returns_same_object():
    """Immutable SafeStrTuple returns itself on deepcopy since no copying is needed."""
    original = SafeStrTuple("a", "b", "c")
    deep_copied = copy.deepcopy(original)
    assert deep_copied is original


def test_non_empty_safe_str_tuple_copy_returns_same_object():
    """Immutable NonEmptySafeStrTuple returns itself on copy."""
    original = NonEmptySafeStrTuple("x", "y", "z")
    copied = copy.copy(original)
    assert copied is original


def test_non_empty_safe_str_tuple_deepcopy_returns_same_object():
    """Immutable NonEmptySafeStrTuple returns itself on deepcopy."""
    original = NonEmptySafeStrTuple("x", "y", "z")
    deep_copied = copy.deepcopy(original)
    assert deep_copied is original


def test_safe_str_tuple_copy_in_deepcopy_memo():
    """SafeStrTuple in a container still returns the same object via deepcopy."""
    original = SafeStrTuple("a", "b")
    container = {"key": original, "list": [original, original]}
    deep_copied = copy.deepcopy(container)

    # All references should point to the same SafeStrTuple instance
    assert deep_copied["key"] is original
    assert deep_copied["list"][0] is original
    assert deep_copied["list"][1] is original


# =============================================================================
# PersiDict copy tests (parametrized across implementations)
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_persidict_copy_creates_new_instance(tmpdir, DictToTest, kwargs):
    """copy.copy() creates a new PersiDict instance, not the same object."""
    original = DictToTest(base_dir=tmpdir, **kwargs)
    copied = copy.copy(original)

    assert copied is not original
    assert type(copied) is type(original)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_persidict_copy_shares_storage(tmpdir, DictToTest, kwargs):
    """Copied PersiDict shares the same underlying storage as the original."""
    original = DictToTest(base_dir=tmpdir, **kwargs)
    original.clear()
    original[("test", "key")] = "value"

    copied = copy.copy(original)

    # Both should see the same data
    assert ("test", "key") in copied
    assert copied[("test", "key")] == "value"
    assert len(copied) == 1

    # Changes through one are visible through the other
    original[("another", "key")] = "another_value"
    assert ("another", "key") in copied
    assert copied[("another", "key")] == "another_value"

    copied[("third", "key")] = "third_value"
    assert ("third", "key") in original
    assert original[("third", "key")] == "third_value"

    original.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_persidict_copy_preserves_parameters(tmpdir, DictToTest, kwargs):
    """Copied PersiDict has the same configuration parameters as the original."""
    original = DictToTest(base_dir=tmpdir, **kwargs)
    copied = copy.copy(original)

    original_params = original.get_params()
    copied_params = copied.get_params()

    # All parameter keys should match
    assert set(original_params.keys()) == set(copied_params.keys())

    # All parameter values should match (except possibly object identity for backends)
    for key in original_params:
        if key == "backend":
            # Backend objects may be shared or recreated depending on implementation
            continue
        assert original_params[key] == copied_params[key], f"Parameter {key} differs"

    original.clear()


# =============================================================================
# Specific backend copy tests
# =============================================================================


def test_file_dir_dict_copy_same_base_dir(tmp_path):
    """FileDirDict copy points to the same base directory."""
    original = FileDirDict(base_dir=tmp_path, serialization_format="pkl")
    original.clear()
    original[("key",)] = {"data": 123}

    copied = copy.copy(original)

    # Should share same base_dir
    assert original.get_params()["base_dir"] == copied.get_params()["base_dir"]

    # Data visible through both
    assert copied[("key",)] == {"data": 123}

    original.clear()


def test_local_dict_copy_shares_backend():
    """LocalDict copy shares the in-memory backend."""
    original = LocalDict(serialization_format="json")
    original[("mem", "key")] = [1, 2, 3]

    copied = copy.copy(original)

    # Should share the same backend object
    original_backend = original.get_params()["backend"]
    copied_backend = copied.get_params()["backend"]
    assert original_backend is copied_backend

    # Changes propagate
    original[("new",)] = "data"
    assert copied[("new",)] == "data"

    original.clear()


@mock_aws
def test_s3_dict_copy_same_bucket(tmp_path):
    """S3Dict copy points to the same bucket."""
    original = S3Dict_FileDirCached(
        base_dir=tmp_path,
        bucket_name="test-bucket",
        serialization_format="pkl",
    )
    original.clear()
    original[("s3", "key")] = "s3_value"

    copied = copy.copy(original)

    assert original.get_params()["bucket_name"] == copied.get_params()["bucket_name"]
    assert copied[("s3", "key")] == "s3_value"

    original.clear()


# =============================================================================
# Edge cases
# =============================================================================


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_persidict_copy_of_empty_dict(tmpdir, DictToTest, kwargs):
    """Copying an empty PersiDict works correctly."""
    original = DictToTest(base_dir=tmpdir, **kwargs)
    original.clear()

    copied = copy.copy(original)

    assert len(copied) == 0
    assert list(copied.keys()) == []

    original.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_persidict_multiple_copies_share_storage(tmpdir, DictToTest, kwargs):
    """Multiple copies all share the same storage."""
    original = DictToTest(base_dir=tmpdir, **kwargs)
    original.clear()

    copy1 = copy.copy(original)
    copy2 = copy.copy(original)
    copy3 = copy.copy(copy1)

    original[("shared",)] = "value"

    assert copy1[("shared",)] == "value"
    assert copy2[("shared",)] == "value"
    assert copy3[("shared",)] == "value"

    original.clear()


def test_safe_str_tuple_empty_copy():
    """Empty SafeStrTuple copy returns same object."""
    original = SafeStrTuple()
    copied = copy.copy(original)
    deep_copied = copy.deepcopy(original)

    assert copied is original
    assert deep_copied is original
    assert len(original) == 0
