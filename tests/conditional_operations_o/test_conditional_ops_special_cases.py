from __future__ import annotations

import pytest

from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.empty_dict import EmptyDict
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import (
    ETAG_HAS_CHANGED,
    ETAG_HAS_NOT_CHANGED,
    KEEP_CURRENT,
    EQUAL_ETAG,
    DIFFERENT_ETAG,
)
from persidict.local_dict import LocalDict
from persidict.write_once_dict import WriteOnceDict


def test_empty_dict_conditional_operations():
    d = EmptyDict()
    with pytest.raises(KeyError):
        d.get_item_if_etag("k", "e", DIFFERENT_ETAG)
    with pytest.raises(KeyError):
        d.get_item_if_etag("k", "e", EQUAL_ETAG)
    with pytest.raises(KeyError):
        d.set_item_if_etag("k", "v", "e", EQUAL_ETAG)
    with pytest.raises(KeyError):
        d.set_item_if_etag("k", "v", "e", DIFFERENT_ETAG)
    with pytest.raises(KeyError):
        d.delete_item_if_etag("k", "e", EQUAL_ETAG)
    with pytest.raises(KeyError):
        d.delete_item_if_etag("k", "e", DIFFERENT_ETAG)
    assert d.discard_item_if_etag("k", "e", EQUAL_ETAG) is False
    assert d.discard_item_if_etag("k", "e", DIFFERENT_ETAG) is False


@pytest.fixture()
def append_only_env(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "main"), append_only=True, serialization_format="json")
    cache = FileDirDict(base_dir=str(tmp_path / "cache"), append_only=True, serialization_format="json")
    wrapper = AppendOnlyDictCached(main, cache)
    return main, cache, wrapper


def test_append_only_set_item_if_etag_equal_validates_etag(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"
    current_etag = wrapper.etag("k")

    assert wrapper.set_item_if_etag("k", KEEP_CURRENT, "bogus", EQUAL_ETAG) is ETAG_HAS_CHANGED
    res = wrapper.set_item_if_etag("k", KEEP_CURRENT, current_etag, EQUAL_ETAG)
    assert res is None or res == wrapper.etag("k")
    assert wrapper["k"] == "v1"


def test_append_only_set_item_if_etag_different_mismatch_raises(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"

    with pytest.raises(ValueError):
        wrapper.set_item_if_etag("k", "v2", "bogus", DIFFERENT_ETAG)


def test_append_only_set_item_if_etag_different_keep_current_mismatch_noop(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["k"] = "v1"
    current_etag = wrapper.etag("k")

    assert wrapper.set_item_if_etag("k", KEEP_CURRENT, "bogus", DIFFERENT_ETAG) is None
    assert wrapper["k"] == "v1"
    assert wrapper.etag("k") == current_etag


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("delete_item_if_etag", ("e", EQUAL_ETAG)),
        ("delete_item_if_etag", ("e", DIFFERENT_ETAG)),
        ("discard_item_if_etag", ("e", EQUAL_ETAG)),
        ("discard_item_if_etag", ("e", DIFFERENT_ETAG)),
    ],
)
def test_append_only_delete_and_discard_not_supported(append_only_env, method_name, args):
    main, cache, wrapper = append_only_env
    method = getattr(wrapper, method_name)
    with pytest.raises(TypeError):
        method("k", *args)


def test_file_dir_append_only_conditional_set_raises(tmp_path):
    d = FileDirDict(base_dir=str(tmp_path / "fd"), append_only=True, serialization_format="json")
    d["k"] = "v1"
    etag = d.etag("k")

    with pytest.raises(KeyError):
        d.set_item_if_etag("k", "v2", etag, EQUAL_ETAG)
    with pytest.raises(KeyError):
        d.set_item_if_etag("k", "v2", "bogus", DIFFERENT_ETAG)


def test_write_once_conditional_ops_not_supported(tmp_path):
    wrapped = LocalDict(append_only=True, serialization_format="json")
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=0.0)

    with pytest.raises(NotImplementedError):
        d.set_item_get_etag("k", "v")
    with pytest.raises(NotImplementedError):
        d.set_item_if_etag("k", "v", "e", EQUAL_ETAG)
    with pytest.raises(NotImplementedError):
        d.set_item_if_etag("k", "v", "e", DIFFERENT_ETAG)

    d["k"] = "v1"
    etag = d.etag("k")
    assert d.get_item_if_etag("k", etag, DIFFERENT_ETAG) is ETAG_HAS_NOT_CHANGED
