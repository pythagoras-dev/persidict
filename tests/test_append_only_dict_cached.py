import time
import pytest

from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.file_dir_dict import FileDirDict
from persidict.persi_dict import PersiDict
from persidict.singletons import ETAG_HAS_NOT_CHANGED
from persidict.singletons import KEEP_CURRENT


@pytest.fixture()
def append_only_env(tmp_path):
    # Use FileDirDict because it exposes digest_len used by the wrapper
    main = FileDirDict(base_dir=str(tmp_path / "main"), append_only=True, serialization_format="json")
    cache = FileDirDict(base_dir=str(tmp_path / "cache"), append_only=True, serialization_format="json")
    wrapper = AppendOnlyDictCached(main, cache)
    return main, cache, wrapper


# --- Mainstream flows -------------------------------------------------------

def test_set_and_get_reads_through_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Write via wrapper
    wrapper[("a", "b")] = {"x": 1}

    # Value is in both main and cache
    assert ("a", "b") in main
    assert ("a", "b") in cache

    # First read should come from cache (already populated)
    assert wrapper[("a", "b")] == {"x": 1}

    # Membership and length reflect main
    assert ("a", "b") in wrapper
    assert len(wrapper) == len(main) == 1

    # Iteration delegated to main (order-agnostic)
    assert set(wrapper.keys()) == set(main.keys())
    assert dict(wrapper.items()) == dict(main.items())


def test_timestamp_passthrough(append_only_env):
    main, cache, wrapper = append_only_env

    wrapper["k"] = 123
    ts_main = main.timestamp(("k",))
    ts_wr = wrapper.timestamp(("k",))
    assert ts_wr == ts_main


def test_get_item_if_etag_changed_populates_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Put directly to main (simulate data appearing outside of cache)
    main[("x",)] = "v1"

    # First call with None etag returns value and etag, and must cache it
    res = wrapper.get_item_if_etag_changed(("x",), None)
    assert res is not ETAG_HAS_NOT_CHANGED
    v, etag = res
    assert v == "v1"
    assert isinstance(etag, (str, type(None)))
    assert ("x",) in cache and cache[("x",)] == "v1"

    # Second call with the same etag should report not changed and keep cache
    res2 = wrapper.get_item_if_etag_changed(("x",), etag)
    assert res2 is ETAG_HAS_NOT_CHANGED
    assert cache[("x",)] == "v1"


def test_set_item_get_etag_mirrors_to_cache(append_only_env):
    main, cache, wrapper = append_only_env

    etag = wrapper.set_item_get_etag(("p",), [1, 2, 3])
    assert ("p",) in main and ("p",) in cache
    assert main[("p",)] == cache[("p",)] == [1, 2, 3]
    # For FileDirDict, etag is derived from timestamp (string)
    assert isinstance(etag, (str, type(None)))


# --- Edge cases and error handling -----------------------------------------

def test_constructor_validation_errors(tmp_path):
    good = FileDirDict(base_dir=str(tmp_path / "m1"), append_only=True, serialization_format="json")
    good2 = FileDirDict(base_dir=str(tmp_path / "m2"), append_only=True, serialization_format="json")

    # Type errors when passing non-PersiDict
    with pytest.raises(TypeError):
        AppendOnlyDictCached(main_dict=123, data_cache=good2)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        AppendOnlyDictCached(main_dict=good, data_cache=None)  # type: ignore[arg-type]

    # ValueError if any is not immutable
    not_immutable = FileDirDict(base_dir=str(tmp_path / "m3"), append_only=False, serialization_format="json")
    with pytest.raises(ValueError):
        AppendOnlyDictCached(main_dict=not_immutable, data_cache=good2)
    with pytest.raises(ValueError):
        AppendOnlyDictCached(main_dict=good, data_cache=not_immutable)

    # base_class_for_values mismatch
    main_int = FileDirDict(base_dir=str(tmp_path / "m4"), append_only=True, serialization_format="json", base_class_for_values=int)
    cache_float = FileDirDict(base_dir=str(tmp_path / "m5"), append_only=True, serialization_format="json", base_class_for_values=float)
    with pytest.raises(ValueError):
        AppendOnlyDictCached(main_dict=main_int, data_cache=cache_float)


def test_deletion_not_supported(append_only_env):
    main, cache, wrapper = append_only_env
    wrapper["a"] = 1
    with pytest.raises(TypeError):
        del wrapper["a"]


def test_attempt_to_modify_existing_key_raises(append_only_env):
    main, cache, wrapper = append_only_env

    wrapper[("k1",)] = "v1"
    with pytest.raises(KeyError):
        wrapper[("k1",)] = "v2"  # append-only


def test_value_type_validation(tmp_path):
    # Create a typed environment: only ints allowed
    main = FileDirDict(base_dir=str(tmp_path / "typed_main"), append_only=True, serialization_format="json", base_class_for_values=int)
    cache = FileDirDict(base_dir=str(tmp_path / "typed_cache"), append_only=True, serialization_format="json", base_class_for_values=int)
    wrapper = AppendOnlyDictCached(main, cache)

    wrapper[("ok",)] = 123
    with pytest.raises(TypeError):
        wrapper[("bad",)] = "str-not-allowed"  # wrong type
    # Ensure no item was written on failure
    assert ("bad",) not in main
    assert ("bad",) not in cache


def test_cache_trusted_without_main_presence(append_only_env):
    main, cache, wrapper = append_only_env

    # Pre-populate cache only; main has no such key
    cache[("only",)] = {"cached": True}

    # __contains__ must trust cache
    assert ("only",) in wrapper

    # __getitem__ must return cached value without probing main
    assert wrapper[("only",)] == {"cached": True}


def test_cache_miss_propagates_keyerror_from_main(append_only_env):
    main, cache, wrapper = append_only_env

    # Ensure neither cache nor main has the key
    key = ("absent",)
    if key in cache:
        # Unlikely, but clean up cache to ensure a cache miss path
        with pytest.raises(KeyError):
            del cache[key]  # FileDirDict will raise on immutable; just ignore
    with pytest.raises(KeyError):
        _ = wrapper[key]


# --- Additional thoroughness tests -----------------------------------------

def test_set_item_get_etag_keep_current_noop(append_only_env):
    main, cache, wrapper = append_only_env

    # No prior value; KEEP_CURRENT should be a no-op and return None
    etag = wrapper.set_item_get_etag(("z",), KEEP_CURRENT)
    assert etag is None
    assert ("z",) not in main
    assert ("z",) not in cache


def test_reject_persidict_value_typeerror(append_only_env):
    main, cache, wrapper = append_only_env

    # Using a PersiDict instance as a value should be rejected by validation
    with pytest.raises(TypeError):
        wrapper[("pd",)] = main  # type: ignore[assignment]


def test_len_and_iteration_ignore_cache_only_keys(append_only_env):
    main, cache, wrapper = append_only_env

    # One item in main
    wrapper[("m",)] = 1
    # Extra cache-only item
    cache[("c-only",)] = 2

    # len() must reflect only main
    assert len(wrapper) == 1

    # Iteration must reflect only main (order-agnostic)
    assert set(wrapper.keys()) == set(main.keys())
    assert dict(wrapper.items()) == dict(main.items())


def test_get_item_if_etag_changed_absent_key_does_not_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Ensure key is absent everywhere
    key = ("nope",)
    assert key not in main and key not in cache

    with pytest.raises((KeyError, FileNotFoundError)):
        wrapper.get_item_if_etag_changed(key, None)

    # Cache should remain untouched
    assert key not in cache





def test_setitem_keep_current_noop(append_only_env):
    main, cache, wrapper = append_only_env
    key = ("keep",)
    wrapper[key] = KEEP_CURRENT
    assert key not in main
    assert key not in cache


def test_setdefault_writes_and_mirrors(append_only_env):
    main, cache, wrapper = append_only_env

    # Missing key: setdefault should write default via wrapper and mirror to cache
    key = ("sd",)
    default = {"d": 1}
    out = wrapper.setdefault(key, default)
    assert out == default
    assert key in main and key in cache
    assert main[key] == cache[key] == default


def test_setdefault_existing_key_returns_existing(append_only_env):
    main, cache, wrapper = append_only_env

    key = ("exists",)
    wrapper[key] = 42  # write through wrapper (also populates cache)

    out = wrapper.setdefault(key, 0)
    assert out == 42
    # No change to stored value
    assert main[key] == cache[key] == 42


def test_setdefault_rejects_joker_default(append_only_env):
    main, cache, wrapper = append_only_env

    key = ("joker",)
    with pytest.raises(TypeError):
        wrapper.setdefault(key, KEEP_CURRENT)  # Joker is not allowed as default
    assert key not in main and key not in cache


def test_etag_matches_main(append_only_env):
    main, cache, wrapper = append_only_env

    key = ("e",)
    wrapper[key] = "v"
    # etag uses timestamp under the hood for FileDirDict; wrapper delegates timestamp to main
    assert wrapper.etag(key) == main.etag(key)


def test_iterators_with_timestamps_delegate(append_only_env):
    # Ensure timestamps differ by sleeping between writes
    from minimum_sleep import min_sleep

    main, cache, wrapper = append_only_env

    k1, k2 = ("t1",), ("t2",)
    wrapper[k1] = 1
    min_sleep(main)
    wrapper[k2] = 2

    # Compare keys_and_timestamps as mappings
    wr_k_ts = dict(wrapper.keys_and_timestamps())
    mn_k_ts = dict(main.keys_and_timestamps())
    assert wr_k_ts.keys() == mn_k_ts.keys()
    for k in wr_k_ts:
        assert isinstance(wr_k_ts[k], float)
        assert abs(wr_k_ts[k] - mn_k_ts[k]) < 1.0  # same fs source; allow tiny drift

    # values_and_timestamps: build mapping by key to (value, ts)
    wr_items_ts = {k: (v, ts) for (k, v, ts) in wrapper.items_and_timestamps()}
    mn_items_ts = {k: (v, ts) for (k, v, ts) in main.items_and_timestamps()}
    assert wr_items_ts == mn_items_ts

    # values_and_timestamps separately (order-insensitive compare)
    def to_sets(iterable):
        return set(iterable)

    assert to_sets(wrapper.values_and_timestamps()) == to_sets(main.values_and_timestamps())



def test_getitem_read_through_populates_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Populate main directly; ensure cache starts empty for this key
    key = ("rt",)
    main[key] = {"v": 1}
    assert key not in cache

    # Read through wrapper should fetch from main and populate cache
    assert wrapper[key] == {"v": 1}
    assert key in cache and cache[key] == {"v": 1}

    # Iteration and length reflect main (order-agnostic)
    assert len(wrapper) == len(main) == 1
    assert set(wrapper.keys()) == set(main.keys())
    assert dict(wrapper.items()) == dict(main.items())
