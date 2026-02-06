from __future__ import annotations

import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import (
    ETAG_HAS_CHANGED,
    ETAG_HAS_NOT_CHANGED,
    EQUAL_ETAG,
    DIFFERENT_ETAG,
)
from persidict.local_dict import LocalDict


@pytest.fixture()
def cached_env(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "main"), serialization_format="json")
    data_cache = LocalDict(serialization_format="json")
    etag_cache = LocalDict(serialization_format="json")
    wrapper = MutableDictCached(main, data_cache, etag_cache)
    return main, data_cache, etag_cache, wrapper


def test_set_item_if_etag_equal_updates_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    res = wrapper.set_item_if_etag("k", "v22", etag, EQUAL_ETAG)
    assert res is not ETAG_HAS_CHANGED
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"
    assert etag_cache["k"] == wrapper.etag("k")

    res_mismatch = wrapper.set_item_if_etag("k", "v33", "bogus", EQUAL_ETAG)
    assert res_mismatch is ETAG_HAS_CHANGED
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"


def test_set_item_if_etag_different_updates_and_preserves_on_match(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    res_match = wrapper.set_item_if_etag("k", "v2", etag, DIFFERENT_ETAG)
    assert res_match is ETAG_HAS_NOT_CHANGED
    assert wrapper["k"] == "v1"

    res = wrapper.set_item_if_etag("k", "v3", "bogus", DIFFERENT_ETAG)
    assert res is not ETAG_HAS_NOT_CHANGED
    assert wrapper["k"] == "v3"
    assert data_cache["k"] == "v3"
    assert etag_cache["k"] == wrapper.etag("k")


def test_delete_item_if_etag_equal_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.delete_item_if_etag("k", "bogus", EQUAL_ETAG) is ETAG_HAS_CHANGED
    assert "k" in main and "k" in data_cache and "k" in etag_cache

    assert wrapper.delete_item_if_etag("k", etag, EQUAL_ETAG) is None
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_delete_item_if_etag_different_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.delete_item_if_etag("k", etag, DIFFERENT_ETAG) is ETAG_HAS_NOT_CHANGED
    assert "k" in main

    assert wrapper.delete_item_if_etag("k", "bogus", DIFFERENT_ETAG) is None
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_equal_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.discard_item_if_etag("k", "bogus", EQUAL_ETAG) is False
    assert "k" in main

    assert wrapper.discard_item_if_etag("k", etag, EQUAL_ETAG) is True
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_different_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.discard_item_if_etag("k", etag, DIFFERENT_ETAG) is False
    assert "k" in main

    assert wrapper.discard_item_if_etag("k", "bogus", DIFFERENT_ETAG) is True
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_get_item_if_etag_equal_refreshes_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    main["k"] = "v1"
    etag = main.etag("k")

    value, new_etag = wrapper.get_item_if_etag("k", etag, EQUAL_ETAG)
    assert value == "v1"
    assert new_etag == etag
    assert data_cache["k"] == "v1"
    assert etag_cache["k"] == etag
