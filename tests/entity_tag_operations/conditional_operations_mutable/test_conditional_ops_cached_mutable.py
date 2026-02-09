from __future__ import annotations

import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ETAG_HAS_CHANGED,
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
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    res = wrapper.set_item_if("k", "v22", etag, ETAG_IS_THE_SAME)
    assert res.condition_was_satisfied
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"
    assert etag_cache["k"] == wrapper.etag("k")

    res_mismatch = wrapper.set_item_if("k", "v33", "bogus", ETAG_IS_THE_SAME)
    assert not res_mismatch.condition_was_satisfied
    assert wrapper["k"] == "v22"
    assert data_cache["k"] == "v22"


def test_set_item_if_etag_different_updates_and_preserves_on_match(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    res_match = wrapper.set_item_if("k", "v2", etag, ETAG_HAS_CHANGED)
    assert not res_match.condition_was_satisfied
    assert wrapper["k"] == "v1"

    res = wrapper.set_item_if("k", "v3", "bogus", ETAG_HAS_CHANGED)
    assert res.condition_was_satisfied
    assert wrapper["k"] == "v3"
    assert data_cache["k"] == "v3"
    assert etag_cache["k"] == wrapper.etag("k")


def test_delete_item_if_etag_equal_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    assert not wrapper.discard_item_if("k", "bogus", ETAG_IS_THE_SAME).condition_was_satisfied
    assert "k" in main and "k" in data_cache and "k" in etag_cache

    assert wrapper.discard_item_if("k", etag, ETAG_IS_THE_SAME).condition_was_satisfied
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_delete_item_if_etag_different_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    assert not wrapper.discard_item_if("k", etag, ETAG_HAS_CHANGED).condition_was_satisfied
    assert "k" in main

    assert wrapper.discard_item_if("k", "bogus", ETAG_HAS_CHANGED).condition_was_satisfied
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_equal_clears_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    assert not wrapper.discard_item_if("k", "bogus", ETAG_IS_THE_SAME).condition_was_satisfied
    assert "k" in main

    assert wrapper.discard_item_if("k", etag, ETAG_IS_THE_SAME).condition_was_satisfied
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_discard_item_if_etag_different_clears_caches_on_mismatch(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    assert not wrapper.discard_item_if("k", etag, ETAG_HAS_CHANGED).condition_was_satisfied
    assert "k" in main

    assert wrapper.discard_item_if("k", "bogus", ETAG_HAS_CHANGED).condition_was_satisfied
    assert "k" not in main
    assert "k" not in data_cache
    assert "k" not in etag_cache


def test_get_item_if_etag_equal_refreshes_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    main["k"] = "v1"
    etag = main.etag("k")

    result = wrapper.get_item_if("k", etag, ETAG_IS_THE_SAME)
    assert result.condition_was_satisfied
    assert result.new_value == "v1"
    assert result.resulting_etag == etag
    assert data_cache["k"] == "v1"
    assert etag_cache["k"] == etag
