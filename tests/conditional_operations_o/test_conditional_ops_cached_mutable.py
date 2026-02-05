from __future__ import annotations

import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import ETAG_HAS_CHANGED, ETAG_HAS_NOT_CHANGED
from persidict.local_dict import LocalDict


@pytest.fixture()
def cached_env(tmp_path):
    main = FileDirDict(base_dir=str(tmp_path / "main"), serialization_format="json")
    data_cache = LocalDict(serialization_format="json")
    etag_cache = LocalDict(serialization_format="json")
    wrapper = MutableDictCached(main, data_cache, etag_cache)
    return main, data_cache, etag_cache, wrapper


def test_set_item_if_etag_not_changed_updates_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    res = wrapper.set_item_if_etag_not_changed("k", "v22", etag)
    assert res is not ETAG_HAS_CHANGED
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"
    assert etag_cache["k"] == wrapper.etag("k")

    res_mismatch = wrapper.set_item_if_etag_not_changed("k", "v33", "bogus")
    assert res_mismatch is ETAG_HAS_CHANGED
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"


def test_set_item_if_etag_changed_updates_and_preserves_on_match(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    res_match = wrapper.set_item_if_etag_changed("k", "v2", etag)
    assert res_match is ETAG_HAS_NOT_CHANGED
    assert wrapper["k"] == "v1"

    res = wrapper.set_item_if_etag_changed("k", "v3", "bogus")
    assert res is not ETAG_HAS_NOT_CHANGED
    assert wrapper["k"] == "v3"
    assert data_cache["k"] == "v3"
    assert etag_cache["k"] == wrapper.etag("k")


def test_delete_item_if_etag_not_changed_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.delete_item_if_etag_not_changed("k", "bogus") is ETAG_HAS_CHANGED
    assert "k" in main and "k" in data_cache and "k" in etag_cache

    assert wrapper.delete_item_if_etag_not_changed("k", etag) is None
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_delete_item_if_etag_changed_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.delete_item_if_etag_changed("k", etag) is ETAG_HAS_NOT_CHANGED
    assert "k" in main

    assert wrapper.delete_item_if_etag_changed("k", "bogus") is None
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_not_changed_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.discard_item_if_etag_not_changed("k", "bogus") is False
    assert "k" in main

    assert wrapper.discard_item_if_etag_not_changed("k", etag) is True
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_changed_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag = wrapper.set_item_get_etag("k", "v1")

    assert wrapper.discard_item_if_etag_changed("k", etag) is False
    assert "k" in main

    assert wrapper.discard_item_if_etag_changed("k", "bogus") is True
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_get_item_if_etag_not_changed_refreshes_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    main["k"] = "v1"
    etag = main.etag("k")

    value, new_etag = wrapper.get_item_if_etag_not_changed("k", etag)
    assert value == "v1"
    assert new_etag == etag
    assert data_cache["k"] == "v1"
    assert etag_cache["k"] == etag
