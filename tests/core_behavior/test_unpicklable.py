"""Tests verifying that PersiDict subclasses cannot be pickled.

PersiDict instances hold references to external resources (files, S3 connections,
etc.) that cannot be meaningfully serialized. The contract requires that all
PersiDict subclasses raise TypeError when pickle is attempted.
"""

import pickle

import pytest

from moto import mock_aws

from persidict import (
    PersiDict,
    FileDirDict,
    LocalDict,
    EmptyDict,
    WriteOnceDict,
    BasicS3Dict,
    S3Dict,
    AppendOnlyDictCached,
    MutableDictCached,
    OverlappingMultiDict,
)


def make_file_dir_dict(tmp_path):
    return FileDirDict(base_dir=str(tmp_path / "fdd"))


def make_local_dict(tmp_path):
    return LocalDict()


def make_empty_dict(tmp_path):
    return EmptyDict()


def make_write_once_dict(tmp_path):
    base = FileDirDict(base_dir=str(tmp_path / "wod"), append_only=True)
    return WriteOnceDict(wrapped_dict=base)


def make_append_only_dict_cached(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "aodc_main"), append_only=True)
    cache = FileDirDict(base_dir=str(tmp_path / "aodc_cache"), append_only=True)
    return AppendOnlyDictCached(main_dict=main, data_cache=cache)


def make_mutable_dict_cached(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "mdc_main"))
    data_cache = FileDirDict(base_dir=str(tmp_path / "mdc_data"))
    etag_cache = FileDirDict(base_dir=str(tmp_path / "mdc_etag"), serialization_format="txt", base_class_for_values=str)
    return MutableDictCached(main_dict=main, data_cache=data_cache, etag_cache=etag_cache)


def make_basic_s3_dict(tmp_path):
    return BasicS3Dict(bucket_name="test-bucket")


def make_s3_dict(tmp_path):
    return S3Dict(bucket_name="test-bucket", base_dir=str(tmp_path / "s3cache"))


# PersiDict subclasses that should be unpicklable
PERSIDICT_SUBCLASS_FACTORIES = [
    ("FileDirDict", make_file_dir_dict),
    ("LocalDict", make_local_dict),
    ("EmptyDict", make_empty_dict),
    ("WriteOnceDict", make_write_once_dict),
    ("AppendOnlyDictCached", make_append_only_dict_cached),
    ("MutableDictCached", make_mutable_dict_cached),
    ("BasicS3Dict", make_basic_s3_dict),
    ("S3Dict", make_s3_dict),
]


@mock_aws
@pytest.mark.parametrize("name,factory", PERSIDICT_SUBCLASS_FACTORIES, ids=[p[0] for p in PERSIDICT_SUBCLASS_FACTORIES])
def test_persidict_subclass_cannot_be_pickled(name, factory, tmp_path):
    """Verify that pickling a PersiDict subclass raises TypeError."""
    instance = factory(tmp_path)
    assert isinstance(instance, PersiDict)
    with pytest.raises(TypeError):
        pickle.dumps(instance)


@mock_aws
@pytest.mark.parametrize("name,factory", PERSIDICT_SUBCLASS_FACTORIES, ids=[p[0] for p in PERSIDICT_SUBCLASS_FACTORIES])
def test_persidict_subclass_setstate_raises(name, factory, tmp_path):
    """Verify that calling __setstate__ on a PersiDict subclass raises TypeError."""
    instance = factory(tmp_path)
    assert isinstance(instance, PersiDict)
    with pytest.raises(TypeError):
        instance.__setstate__({})


def test_overlapping_multi_dict_cannot_be_pickled(tmp_path):
    """Verify that OverlappingMultiDict cannot be pickled.

    Note: OverlappingMultiDict is not a PersiDict subclass but still
    prevents pickling as it contains PersiDict instances.
    """
    instance = OverlappingMultiDict(
        dict_type=FileDirDict,
        shared_subdicts_params=dict(base_dir=str(tmp_path / "omd")),
        pkl={},
        json={},
    )
    with pytest.raises(TypeError):
        pickle.dumps(instance)


def test_overlapping_multi_dict_setstate_raises(tmp_path):
    """Verify that calling __setstate__ on OverlappingMultiDict raises TypeError."""
    instance = OverlappingMultiDict(
        dict_type=FileDirDict,
        shared_subdicts_params=dict(base_dir=str(tmp_path / "omd")),
        pkl={},
    )
    with pytest.raises(TypeError):
        instance.__setstate__({})
