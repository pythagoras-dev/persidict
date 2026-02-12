import time
import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.persi_dict import PersiDict
from persidict.local_dict import LocalDict
from persidict.safe_str_tuple import NonEmptySafeStrTuple
from persidict.jokers_and_status_flags import (
    KEEP_CURRENT,
    DELETE_CURRENT,
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
    ITEM_NOT_AVAILABLE,
)


class FakeETagMain(PersiDict):
    """A simple in-memory PersiDict with native ETags for testing.

    - Stores values in a dict mapping key tuple -> (value, etag:str, timestamp:float)
    - ETag is a monotonically increasing string per write.
    - Supports required APIs used by ETaggableDictCached.
    """
    def __init__(self, *, base_class_for_values=None, serialization_format: str = "pkl"):
        super().__init__(append_only=False,
                         base_class_for_values=base_class_for_values,
                         serialization_format=serialization_format)
        # Provide digest_len attribute expected by cache adapter
        self.digest_len = 0
        self._store = {}
        self._counter = 0

    def __contains__(self, key) -> bool:
        key = NonEmptySafeStrTuple(key)
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)

    def _generic_iter(self, result_type: set[str]):
        # very simple iterator over items
        self._process_generic_iter_args(result_type)
        def gen():
            for k, (v, _e, ts) in list(self._store.items()):
                out = []
                if "keys" in result_type:
                    out.append(k)
                if "values" in result_type:
                    out.append(v)
                if "timestamps" in result_type:
                    out.append(ts)
                if len(out) == 1:
                    yield out[0]
                else:
                    yield tuple(out)
        return gen()

    def timestamp(self, key) -> float:
        key = NonEmptySafeStrTuple(key)
        if key not in self._store:
            raise KeyError(key)
        return self._store[key][2]

    def etag(self, key) -> str:
        key = NonEmptySafeStrTuple(key)
        if key not in self._store:
            raise KeyError(key)
        return self._store[key][1]

    def __getitem__(self, key):
        key = NonEmptySafeStrTuple(key)
        if key not in self._store:
            raise KeyError(key)
        return self._store[key][0]

    def __setitem__(self, key, value):
        if self._process_setitem_args(key, value) is not None:
            pass
        key = NonEmptySafeStrTuple(key)
        self._counter += 1
        etag = f"E{self._counter}"
        ts = time.time()
        self._store[key] = (value, etag, ts)

    def __delitem__(self, key):
        key = NonEmptySafeStrTuple(key)
        if key in self._store:
            del self._store[key]


@pytest.fixture()
def cached_env():
    main = FakeETagMain()
    data_cache = LocalDict()
    etag_cache = LocalDict(serialization_format="json")  # store etags as str
    wrapper = MutableDictCached(main, data_cache, etag_cache)
    return main, data_cache, etag_cache, wrapper


def test_constructor_validations():
    main = FakeETagMain()
    good_cache = LocalDict()
    # immutable caches are not allowed
    with pytest.raises(ValueError):
        MutableDictCached(main, LocalDict(append_only=True), good_cache)
    with pytest.raises(ValueError):
        MutableDictCached(main, good_cache, LocalDict(append_only=True))
    # all must be PersiDict
    with pytest.raises(TypeError):
        MutableDictCached(main, {}, good_cache)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        MutableDictCached(main, good_cache, {})  # type: ignore[arg-type]


def test_setitem_and_caches_updated(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper[("a",)] = 123
    etag = wrapper.etag(("a",))
    assert ("a",) in wrapper
    assert wrapper[("a",)] == 123
    assert data_cache[("a",)] == 123
    assert etag_cache[("a",)] == etag


def test_getitem_read_through_and_cache_population(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    # Populate main directly (simulating out-of-band write)
    main[("k1",)] = "v1"
    etag = main.etag(("k1",))
    # First access should fetch from main and populate caches
    assert wrapper[("k1",)] == "v1"
    assert data_cache[("k1",)] == "v1"
    assert etag_cache[("k1",)] == etag

    # Now access again: since etag matches, value should be taken from cache.
    # To verify fallback path, drop the cached value but keep etag
    del data_cache[("k1",)]
    assert ("k1",) not in data_cache
    # __getitem__ should still return and repopulate the cache
    assert wrapper[("k1",)] == "v1"
    assert data_cache[("k1",)] == "v1"


def test_get_item_if_etag_different_semantics(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper["key"] = {"a": 1}
    etag1 = wrapper.etag("key")
    # Ask with current etag: must return sentinel and not modify caches
    res = wrapper.get_item_if("key", ETAG_HAS_CHANGED, etag1)
    assert not res.condition_was_satisfied
    # Update value -> new etag; call with old etag should return new value and update caches
    wrapper["key"] = {"a": 2}
    etag2 = wrapper.etag("key")
    assert etag2 != etag1
    res2 = wrapper.get_item_if("key", ETAG_HAS_CHANGED, etag1)
    assert res2.condition_was_satisfied
    assert res2.new_value == {"a": 2}
    assert res2.resulting_etag == etag2
    assert data_cache["key"] == {"a": 2}
    assert etag_cache["key"] == etag2


def test_contains_len_iter_delegate_to_main(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    main[("x",)] = 1
    main[("y",)] = 2
    assert ("x",) in wrapper and ("y",) in wrapper
    assert len(wrapper) == 2
    assert set(wrapper.keys()) == {NonEmptySafeStrTuple(("x",)), NonEmptySafeStrTuple(("y",))}
    assert set(dict(wrapper.items()).values()) == {1, 2}


def test_delete_removes_from_main_and_caches(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper[("d",)] = 7
    assert ("d",) in main
    assert ("d",) in data_cache
    assert ("d",) in etag_cache
    del wrapper[("d",)]
    assert ("d",) not in main
    assert ("d",) not in data_cache
    assert ("d",) not in etag_cache


def test_wrapper_write_update_and_jokers(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # initial write via wrapper
    wrapper[("w",)] = {"v": 1}
    e1 = wrapper.etag(("w",))
    assert etag_cache[("w",)] == e1
    assert data_cache[("w",)] == {"v": 1}

    # overwrite via wrapper -> new etag and caches updated
    wrapper[("w",)] = {"v": 2}
    e2 = wrapper.etag(("w",))
    assert e2 != e1
    assert etag_cache[("w",)] == e2
    assert data_cache[("w",)] == {"v": 2}
    assert wrapper[("w",)] == {"v": 2}

    # KEEP_CURRENT should not change anything and return None
    prev_val = wrapper[("w",)]
    prev_etag = etag_cache[("w",)]
    wrapper[("w",)] = KEEP_CURRENT
    assert wrapper[("w",)] == prev_val
    assert etag_cache[("w",)] == prev_etag
    assert data_cache[("w",)] == prev_val

    # DELETE_CURRENT via set_item_get_etag should remove from main and caches
    wrapper[("w",)] = DELETE_CURRENT
    assert ("w",) not in main
    assert ("w",) not in etag_cache
    assert ("w",) not in data_cache

    # DELETE_CURRENT via __setitem__ syntax
    wrapper[("z",)] = 10
    assert ("z",) in main
    wrapper[("z",)] = DELETE_CURRENT
    assert ("z",) not in main
    assert ("z",) not in etag_cache
    assert ("z",) not in data_cache


def test_external_modifications_etag_refresh_and_delete(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Set externally in main
    main[("ext",)] = "v1"
    e1 = main.etag(("ext",))
    # Read through wrapper populates caches
    assert wrapper[("ext",)] == "v1"
    assert etag_cache[("ext",)] == e1
    assert data_cache[("ext",)] == "v1"

    # External update in main should cause refresh on next read
    main[("ext",)] = "v2"
    e2 = main.etag(("ext",))
    assert e2 != e1
    assert wrapper[("ext",)] == "v2"  # triggers refresh
    assert etag_cache[("ext",)] == e2
    assert data_cache[("ext",)] == "v2"

    # External delete: wrapper should reflect KeyError on get and membership False
    del main[("ext",)]
    assert ("ext",) not in wrapper  # __contains__ delegates to main
    with pytest.raises(KeyError):
        _ = wrapper[("ext",)]


def test_cache_edge_cases_missing_etag_or_value(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Seed via wrapper
    wrapper[("c",)] = 111
    e1 = wrapper.etag(("c",))
    assert etag_cache[("c",)] == e1
    assert data_cache[("c",)] == 111

    # Case A: etag missing but data present -> force fetch from main and re-store etag
    del etag_cache[("c",)]
    assert ("c",) in data_cache and ("c",) not in etag_cache
    # Access triggers fetch with ITEM_NOT_AVAILABLE; value same, etag restored
    assert wrapper[("c",)] == 111
    assert ("c",) in etag_cache

    # Case B: data missing but stale etag present and main changed -> refresh both
    main[("c",)] = 222
    e2 = main.etag(("c",))  # external update
    del data_cache[("c",)]
    assert ("c",) not in data_cache and etag_cache[("c",)] != e2
    assert wrapper[("c",)] == 222
    assert data_cache[("c",)] == 222
    assert etag_cache[("c",)] == e2

    # Case C: corrupted etag type in cache should be treated as mismatch and refresh
    etag_cache[("c",)] = 123456  # wrong type instead of str
    main[("c",)] = 333
    e3 = main.etag(("c",))
    val = wrapper[("c",)]
    assert val == 333
    assert etag_cache[("c",)] == e3  # corrected to proper etag from main


def test_keyerror_when_main_missing_even_if_cache_has_entries(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Manually put stale entries into caches for a non-existent key
    data_cache[("ghost",)] = "shadow"
    etag_cache[("ghost",)] = "E999"
    assert ("ghost",) not in main

    with pytest.raises(KeyError):
        _ = wrapper[("ghost",)]

    # Missing key results should purge stale cache entries
    assert ("ghost",) not in data_cache
    assert ("ghost",) not in etag_cache


def test_constructor_main_type_validation():
    good_cache = LocalDict()
    # main must be a PersiDict too
    with pytest.raises(TypeError):
        MutableDictCached({}, good_cache, good_cache)  # type: ignore[arg-type]


def test_base_class_for_values_enforced_via_wrapper():
    # Main enforces dict values; caches accept any
    main = FakeETagMain(base_class_for_values=dict)
    data_cache = LocalDict()
    etag_cache = LocalDict(serialization_format="json")
    wrapper = MutableDictCached(main, data_cache, etag_cache)

    # Wrapper should mirror main's base_class_for_values
    assert wrapper.base_class_for_values is dict

    # Storing wrong type via wrapper must raise TypeError
    with pytest.raises(TypeError):
        wrapper[("bcv",)] = 123

    # Correct type succeeds and updates caches
    wrapper[("bcv",)] = {"a": 1}
    etag = wrapper.etag(("bcv",))
    assert etag_cache[("bcv",)] == etag
    assert data_cache[("bcv",)] == {"a": 1}



def test_timestamp_delegation_to_main(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Set via main to simulate out-of-band write
    main[("ts",)] = "v1"

    # Timestamps should match between wrapper and main
    ts_main = main.timestamp(("ts",))
    ts_wrap = wrapper.timestamp(("ts",))
    assert ts_wrap == ts_main

    # After external update, timestamp should increase and still match
    time.sleep(0.001)
    main[("ts",)] = "v2"
    ts_main2 = main.timestamp(("ts",))
    ts_wrap2 = wrapper.timestamp(("ts",))
    assert ts_wrap2 == ts_main2
    assert ts_main2 >= ts_main



def test_setdefault_if_insert_updates_caches(cached_env):
    """Verify setdefault_if delegates to main_dict and syncs caches on insert."""
    main, data_cache, etag_cache, wrapper = cached_env

    res = wrapper.setdefault_if(
        ("sd",), "default_val", ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE)

    assert res.condition_was_satisfied
    assert wrapper[("sd",)] == "default_val"
    assert data_cache[("sd",)] == "default_val"
    assert etag_cache[("sd",)] == res.resulting_etag


def test_setdefault_if_existing_key_preserves_caches(cached_env):
    """Verify setdefault_if does not mutate when key exists; caches reflect existing value."""
    main, data_cache, etag_cache, wrapper = cached_env
    wrapper[("sd2",)] = "original"
    etag = wrapper.etag(("sd2",))

    res = wrapper.setdefault_if(
        ("sd2",), "ignored", ETAG_IS_THE_SAME, etag)

    assert res.condition_was_satisfied
    assert res.new_value == "original"
    assert res.resulting_etag == etag
    assert data_cache[("sd2",)] == "original"
    assert etag_cache[("sd2",)] == etag


def test_setdefault_if_absent_condition_fails_no_cache_pollution(cached_env):
    """Verify setdefault_if with unsatisfied condition leaves caches empty."""
    main, data_cache, etag_cache, wrapper = cached_env

    res = wrapper.setdefault_if(
        ("sd3",), "val", ETAG_HAS_CHANGED, ITEM_NOT_AVAILABLE)

    assert not res.condition_was_satisfied
    assert ("sd3",) not in main
    assert ("sd3",) not in data_cache
    assert ("sd3",) not in etag_cache


def test_contains_ignores_cache_only_ghosts(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Put ghost entries only in caches
    data_cache[("ghost2",)] = "shadow"
    etag_cache[("ghost2",)] = "E123"
    assert ("ghost2",) not in main

    # __contains__ must reflect main only, ignoring cache-only ghosts
    assert ("ghost2",) not in wrapper


def test_getitem_etag_warm_data_cold_repopulates_both_caches(cached_env):
    """When etag_cache has an entry but data_cache does not, __getitem__
    should fetch from main in a single call and repopulate both caches."""
    main, data_cache, etag_cache, wrapper = cached_env

    wrapper[("k",)] = "v1"
    etag = wrapper.etag(("k",))
    assert data_cache[("k",)] == "v1"
    assert etag_cache[("k",)] == etag

    # Evict only the data cache, simulating independent eviction
    del data_cache[("k",)]
    assert ("k",) not in data_cache
    assert etag_cache[("k",)] == etag

    # Read should succeed and repopulate both caches
    assert wrapper[("k",)] == "v1"
    assert data_cache[("k",)] == "v1"
    assert etag_cache[("k",)] == etag


def test_getitem_etag_warm_data_cold_key_deleted_raises_and_purges(cached_env):
    """When etag_cache has an entry but data_cache does not and the key
    was deleted from main, __getitem__ should raise KeyError and purge
    the stale etag_cache entry."""
    main, data_cache, etag_cache, wrapper = cached_env

    wrapper[("k",)] = "v1"
    assert ("k",) in etag_cache

    # Remove from data cache and main, leaving only etag cache
    del data_cache[("k",)]
    del main[("k",)]
    assert ("k",) not in data_cache
    assert ("k",) not in main
    assert ("k",) in etag_cache

    with pytest.raises(KeyError):
        _ = wrapper[("k",)]

    # Stale etag cache entry should be purged
    assert ("k",) not in etag_cache
