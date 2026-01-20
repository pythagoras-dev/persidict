"""Tests for __eq__ behavior across PersiDict implementations."""

import pytest
from moto import mock_aws

from persidict import FileDirDict, LocalDict, BasicS3Dict


def test_equality_same_params_same_backend_localdict():
    """Verify two LocalDicts with same params pointing to same backend are equal."""
    d1 = LocalDict(serialization_format="json")
    d1["key1"] = "value1"

    # Create second dict with same params (shares backend)
    params = d1.get_params()
    d2 = LocalDict(**params)

    assert d1 == d2


def test_equality_same_params_same_backend_filedirdict(tmpdir):
    """Verify two FileDirDicts with same params pointing to same backend are equal."""
    d1 = FileDirDict(base_dir=tmpdir, serialization_format="json")
    d1["key1"] = "value1"

    params = d1.get_params()
    d2 = FileDirDict(**params)

    assert d1 == d2


def test_equality_different_content_localdict():
    """Verify LocalDicts with different content are not equal."""
    d1 = LocalDict(serialization_format="json")
    d2 = LocalDict(serialization_format="json")

    d1["key1"] = "value1"
    d2["key1"] = "different_value"

    assert not (d1 == d2)


def test_equality_different_content_filedirdict(tmpdir):
    """Verify FileDirDicts with different content are not equal."""
    d1 = FileDirDict(base_dir=tmpdir.mkdir("d1"), serialization_format="json")
    d2 = FileDirDict(base_dir=tmpdir.mkdir("d2"), serialization_format="json")

    d1["key1"] = "value1"
    d2["key1"] = "different_value"

    assert not (d1 == d2)


def test_equality_different_keys_localdict():
    """Verify LocalDicts with different keys are not equal."""
    d1 = LocalDict(serialization_format="json")
    d2 = LocalDict(serialization_format="json")

    d1["key1"] = "value"
    d2["key2"] = "value"

    assert not (d1 == d2)


def test_equality_different_keys_filedirdict(tmpdir):
    """Verify FileDirDicts with different keys are not equal."""
    d1 = FileDirDict(base_dir=tmpdir.mkdir("d1"), serialization_format="json")
    d2 = FileDirDict(base_dir=tmpdir.mkdir("d2"), serialization_format="json")

    d1["key1"] = "value"
    d2["key2"] = "value"

    assert not (d1 == d2)


def test_equality_empty_dicts_localdict():
    """Verify two empty LocalDicts are equal by content."""
    d1 = LocalDict(serialization_format="json")
    d2 = LocalDict(serialization_format="json")

    assert d1 == d2


def test_equality_empty_dicts_filedirdict(tmpdir):
    """Verify two empty FileDirDicts are equal by content."""
    d1 = FileDirDict(base_dir=tmpdir.mkdir("d1"), serialization_format="json")
    d2 = FileDirDict(base_dir=tmpdir.mkdir("d2"), serialization_format="json")

    assert d1 == d2


def test_equality_with_regular_dict():
    """Verify PersiDict can be compared with regular dict by content."""
    d = LocalDict(serialization_format="json")
    d["key1"] = "value1"
    d["key2"] = "value2"

    regular = {"key1": "value1", "key2": "value2"}

    # Should compare equal by content (fallback comparison)
    assert d == regular


def test_equality_cross_type_localdict_filedirdict(tmpdir):
    """Verify LocalDict and FileDirDict with same content compare equal."""
    local = LocalDict(serialization_format="json")
    file_dir = FileDirDict(base_dir=tmpdir, serialization_format="json")

    local["key1"] = "value1"
    file_dir["key1"] = "value1"

    # Different types but same content - should be equal via content comparison
    assert local == file_dir


def test_equality_different_length():
    """Verify dicts with different lengths are not equal."""
    d1 = LocalDict(serialization_format="json")
    d2 = LocalDict(serialization_format="json")

    d1["key1"] = "value1"
    d1["key2"] = "value2"
    d2["key1"] = "value1"

    assert not (d1 == d2)


def test_equality_same_content_different_serialization():
    """Verify dicts with same content but different formats compare equal by content."""
    d1 = LocalDict(serialization_format="json")
    d2 = LocalDict(serialization_format="pkl")

    d1["key1"] = "value1"
    d2["key1"] = "value1"

    # Different params, but same content - should be equal via content fallback
    assert d1 == d2


@mock_aws
def test_equality_s3dict_same_backend():
    """Verify S3Dicts with same params pointing to same bucket are equal."""
    d1 = BasicS3Dict(bucket_name="test_bucket", serialization_format="json")
    d1["key1"] = "value1"

    params = d1.get_params()
    d2 = BasicS3Dict(**params)

    assert d1 == d2


@mock_aws
def test_equality_s3dict_different_buckets():
    """Verify S3Dicts with different buckets containing same content are equal by content."""
    d1 = BasicS3Dict(bucket_name="bucket1", serialization_format="json")
    d2 = BasicS3Dict(bucket_name="bucket2", serialization_format="json")

    d1["key1"] = "value1"
    d2["key1"] = "value1"

    # Different buckets, same content - should be equal via content comparison
    assert d1 == d2


@mock_aws
def test_equality_s3dict_different_content():
    """Verify S3Dicts with different content are not equal."""
    d1 = BasicS3Dict(bucket_name="bucket1", serialization_format="json")
    d2 = BasicS3Dict(bucket_name="bucket2", serialization_format="json")

    d1["key1"] = "value1"
    d2["key1"] = "different_value"

    assert not (d1 == d2)
