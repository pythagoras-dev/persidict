import time
import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.persi_dict import PersiDict
from persidict.local_dict import LocalDict
from persidict.safe_str_tuple import NonEmptySafeStrTuple
from persidict.singletons import ETAG_HAS_NOT_CHANGED


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

    def get_item_if_etag_changed(self, key, etag):
        key = NonEmptySafeStrTuple(key)
        if key not in self._store:
            raise KeyError(key)
        current_etag = self._store[key][1]
        if etag == current_etag:
            return ETAG_HAS_NOT_CHANGED
        else:
            return self._store[key][0], current_etag

    def set_item_get_etag(self, key, value):
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is not None:
            # PersiDict._process_setitem_args returns a StatusFlag; only when
            # EXECUTION_IS_COMPLETE do we short-circuit. For simplicity, delegate
            # to PersiDict __setitem__ semantics by handling DELETE_CURRENT/KEEP_CURRENT
            # via _process_setitem_args and fall through when normal execution.
            pass
        self._counter += 1
        etag = f"E{self._counter}"
        ts = time.time()
        self._store[key] = (value, etag, ts)
        return etag

    def __setitem__(self, key, value):
        # Use set_item_get_etag for behavior symmetry
        self.set_item_get_etag(key, value)

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
    etag = wrapper.set_item_get_etag(("a",), 123)
    assert ("a",) in wrapper
    assert wrapper[("a",)] == 123
    assert data_cache[("a",)] == 123
    assert etag_cache[("a",)] == etag


def test_getitem_read_through_and_cache_population(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    # Populate main directly (simulating out-of-band write)
    etag = main.set_item_get_etag(("k1",), "v1")
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


def test_get_item_if_etag_changed_semantics(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    etag1 = wrapper.set_item_get_etag("key", {"a": 1})
    # Ask with current etag: must return sentinel and not modify caches
    res = wrapper.get_item_if_etag_changed("key", etag1)
    assert res is ETAG_HAS_NOT_CHANGED
    # Update value -> new etag; call with old etag should return new value and update caches
    etag2 = wrapper.set_item_get_etag("key", {"a": 2})
    assert etag2 != etag1
    val, new_etag = wrapper.get_item_if_etag_changed("key", etag1)
    assert val == {"a": 2}
    assert new_etag == etag2
    assert data_cache["key"] == {"a": 2}
    assert etag_cache["key"] == etag2


def test_contains_len_iter_delegate_to_main(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env
    main.set_item_get_etag(("x",), 1)
    main.set_item_get_etag(("y",), 2)
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


from persidict.singletons import KEEP_CURRENT, DELETE_CURRENT


def test_wrapper_write_update_and_jokers(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # initial write via wrapper
    e1 = wrapper.set_item_get_etag(("w",), {"v": 1})
    assert etag_cache[("w",)] == e1
    assert data_cache[("w",)] == {"v": 1}

    # overwrite via wrapper -> new etag and caches updated
    e2 = wrapper.set_item_get_etag(("w",), {"v": 2})
    assert e2 != e1
    assert etag_cache[("w",)] == e2
    assert data_cache[("w",)] == {"v": 2}
    assert wrapper[("w",)] == {"v": 2}

    # KEEP_CURRENT should not change anything and return None
    prev_val = wrapper[("w",)]
    prev_etag = etag_cache[("w",)]
    res = wrapper.set_item_get_etag(("w",), KEEP_CURRENT)
    assert res is None
    assert wrapper[("w",)] == prev_val
    assert etag_cache[("w",)] == prev_etag
    assert data_cache[("w",)] == prev_val

    # DELETE_CURRENT via set_item_get_etag should remove from main and caches
    res2 = wrapper.set_item_get_etag(("w",), DELETE_CURRENT)
    assert res2 is None
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
    e1 = main.set_item_get_etag(("ext",), "v1")
    # Read through wrapper populates caches
    assert wrapper[("ext",)] == "v1"
    assert etag_cache[("ext",)] == e1
    assert data_cache[("ext",)] == "v1"

    # External update in main should cause refresh on next read
    e2 = main.set_item_get_etag(("ext",), "v2")
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
    e1 = wrapper.set_item_get_etag(("c",), 111)
    assert etag_cache[("c",)] == e1
    assert data_cache[("c",)] == 111

    # Case A: etag missing but data present -> force fetch from main and re-store etag
    del etag_cache[("c",)]
    assert ("c",) in data_cache and ("c",) not in etag_cache
    # Access triggers fetch with etag=None; value same, etag restored
    assert wrapper[("c",)] == 111
    assert ("c",) in etag_cache

    # Case B: data missing but stale etag present and main changed -> refresh both
    e2 = main.set_item_get_etag(("c",), 222)  # external update
    del data_cache[("c",)]
    assert ("c",) not in data_cache and etag_cache[("c",)] != e2
    assert wrapper[("c",)] == 222
    assert data_cache[("c",)] == 222
    assert etag_cache[("c",)] == e2

    # Case C: corrupted etag type in cache should be treated as mismatch and refresh
    etag_cache[("c",)] = 123456  # wrong type instead of str
    e3 = main.set_item_get_etag(("c",), 333)
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

    # Caches may still contain stale data; ensure they are not relied upon
    assert ("ghost",) in data_cache
    assert ("ghost",) in etag_cache


def test_constructor_main_type_validation():
    main = FakeETagMain()
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
        wrapper.set_item_get_etag(("bcv",), 123)

    # Correct type succeeds and updates caches
    etag = wrapper.set_item_get_etag(("bcv",), {"a": 1})
    assert etag_cache[("bcv",)] == etag
    assert data_cache[("bcv",)] == {"a": 1}



def test_timestamp_delegation_to_main(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Set via main to simulate out-of-band write
    main.set_item_get_etag(("ts",), "v1")

    # Timestamps should match between wrapper and main
    ts_main = main.timestamp(("ts",))
    ts_wrap = wrapper.timestamp(("ts",))
    assert ts_wrap == ts_main

    # After external update, timestamp should increase and still match
    time.sleep(0.001)
    main.set_item_get_etag(("ts",), "v2")
    ts_main2 = main.timestamp(("ts",))
    ts_wrap2 = wrapper.timestamp(("ts",))
    assert ts_wrap2 == ts_main2
    assert ts_main2 >= ts_main



def test_contains_ignores_cache_only_ghosts(cached_env):
    main, data_cache, etag_cache, wrapper = cached_env

    # Put ghost entries only in caches
    data_cache[("ghost2",)] = "shadow"
    etag_cache[("ghost2",)] = "E123"
    assert ("ghost2",) not in main

    # __contains__ must reflect main only, ignoring cache-only ghosts
    assert ("ghost2",) not in wrapper
