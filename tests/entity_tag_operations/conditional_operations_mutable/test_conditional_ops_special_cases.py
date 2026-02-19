from __future__ import annotations

import pytest

from persidict import MutationPolicyError
from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.empty_dict import EmptyDict
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import (
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
    KEEP_CURRENT,
    ConditionalOperationResult,
)
from persidict.local_dict import LocalDict
from persidict.write_once_dict import WriteOnceDict


def test_empty_dict_conditional_operations():
    d = EmptyDict()
    # get_item_if on empty dict: key absent, actual_etag=ITEM_NOT_AVAILABLE
    res = d.get_item_if("k", condition=ETAG_HAS_CHANGED, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE

    res = d.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE

    # set_item_if on empty dict: key absent, actual_etag=ITEM_NOT_AVAILABLE
    res = d.set_item_if("k", value="v", condition=ETAG_IS_THE_SAME, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE

    res = d.set_item_if("k", value="v", condition=ETAG_HAS_CHANGED, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE

    # discard_if on empty dict: key absent
    res = d.discard_if("k", condition=ETAG_IS_THE_SAME, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE

    res = d.discard_if("k", condition=ETAG_HAS_CHANGED, expected_etag="e")
    assert isinstance(res, ConditionalOperationResult)
    assert res.actual_etag is ITEM_NOT_AVAILABLE


@pytest.fixture()
def append_only_env(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "main"), append_only=True, serialization_format="json")
    cache = FileDirDict(base_dir=str(tmp_path / "cache"), append_only=True, serialization_format="json")
    wrapper = AppendOnlyDictCached(main_dict=main, data_cache=cache)
    return main, cache, wrapper


def test_append_only_set_item_if_etag_equal_validates_etag(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"
    current_etag = wrapper.etag("k")

    res = wrapper.set_item_if("k", value=KEEP_CURRENT, condition=ETAG_IS_THE_SAME, expected_etag="bogus")
    assert not res.condition_was_satisfied
    res = wrapper.set_item_if("k", value=KEEP_CURRENT, condition=ETAG_IS_THE_SAME, expected_etag=current_etag)
    assert res.condition_was_satisfied
    assert wrapper["k"] == "v1"


def test_append_only_set_item_if_etag_different_mismatch_raises(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"

    with pytest.raises(MutationPolicyError):
        wrapper.set_item_if("k", value="v2", condition=ETAG_HAS_CHANGED, expected_etag="bogus")


def test_append_only_set_item_if_etag_different_keep_current_mismatch_noop(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"
    current_etag = wrapper.etag("k")

    res = wrapper.set_item_if("k", value=KEEP_CURRENT, condition=ETAG_HAS_CHANGED, expected_etag="bogus")
    assert res.condition_was_satisfied
    assert wrapper["k"] == "v1"
    assert wrapper.etag("k") == current_etag


@pytest.mark.parametrize(
    "method_name,kwargs",
    [
        ("discard_if", dict(condition=ETAG_IS_THE_SAME, expected_etag="e")),
        ("discard_if", dict(condition=ETAG_HAS_CHANGED, expected_etag="e")),
    ],
)
def test_append_only_delete_and_discard_not_supported(append_only_env, method_name, kwargs):
    main, cache, wrapper = append_only_env
    method = getattr(wrapper, method_name)
    with pytest.raises(MutationPolicyError):
        method("k", **kwargs)


def test_file_dir_append_only_conditional_set_raises(tmp_path):
    d = FileDirDict(base_dir=str(tmp_path / "fd"), append_only=True, serialization_format="json")
    d["k"] = "v1"
    etag = d.etag("k")

    with pytest.raises(MutationPolicyError):
        d.set_item_if("k", value="v2", condition=ETAG_IS_THE_SAME, expected_etag=etag)
    with pytest.raises(MutationPolicyError):
        d.set_item_if("k", value="v2", condition=ETAG_HAS_CHANGED, expected_etag="bogus")


def test_write_once_conditional_ops_not_supported(tmp_path):
    wrapped = LocalDict(append_only=True, serialization_format="json")
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=0.0)

    with pytest.raises(MutationPolicyError):
        d.set_item_if("k", value="v", condition=ETAG_IS_THE_SAME, expected_etag="e")
    with pytest.raises(MutationPolicyError):
        d.set_item_if("k", value="v", condition=ETAG_HAS_CHANGED, expected_etag="e")

    d["k"] = "v1"
    etag = d.etag("k")
    res = d.get_item_if("k", condition=ETAG_HAS_CHANGED, expected_etag=etag)
    assert not res.condition_was_satisfied
