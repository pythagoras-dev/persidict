import pytest

from persidict import MutationPolicyError
from persidict.cached_appendonly_dict import AppendOnlyDictCached
from persidict.file_dir_dict import FileDirDict
from persidict.jokers_and_status_flags import (
    ITEM_NOT_AVAILABLE,
    ETAG_HAS_CHANGED,
    ETAG_IS_THE_SAME,
)
from persidict.jokers_and_status_flags import KEEP_CURRENT


@pytest.fixture()
def append_only_env(tmp_path):
    # Use FileDirDict because it exposes digest_len used by the wrapper
    main = FileDirDict(base_dir=str(tmp_path / "main"), append_only=True, serialization_format="json")
    cache = FileDirDict(base_dir=str(tmp_path / "cache"), append_only=True, serialization_format="json")
    wrapper = AppendOnlyDictCached(main_dict=main, data_cache=cache)
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


def test_get_item_if_etag_different_populates_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Put directly to main (simulate data appearing outside of cache)
    main[("x",)] = "v1"

    # First call with ITEM_NOT_AVAILABLE returns value and etag, and must cache it
    res = wrapper.get_item_if(("x",), condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)
    assert res.condition_was_satisfied
    v = res.new_value
    etag = res.resulting_etag
    assert v == "v1"
    assert isinstance(etag, (str, type(None)))
    assert ("x",) in cache and cache[("x",)] == "v1"

    # Second call with the same etag should report not changed and keep cache
    res2 = wrapper.get_item_if(("x",), condition=ETAG_HAS_CHANGED, expected_etag=etag)
    assert not res2.condition_was_satisfied
    assert cache[("x",)] == "v1"


def test_set_item_get_etag_mirrors_to_cache(append_only_env):
    main, cache, wrapper = append_only_env

    wrapper[("p",)] = [1, 2, 3]
    etag = wrapper.etag(("p",))
    assert ("p",) in main and ("p",) in cache
    assert main[("p",)] == cache[("p",)] == [1, 2, 3]
    # For FileDirDict, etag is derived from timestamp (string)
    assert isinstance(etag, (str, type(None)))


def test_set_item_if_failed_condition_populates_cache(append_only_env):
    main, cache, wrapper = append_only_env

    main[("k",)] = "v1"
    assert ("k",) not in cache

    res = wrapper.set_item_if(("k",), value=KEEP_CURRENT, condition=ETAG_IS_THE_SAME, expected_etag="bogus")

    assert not res.condition_was_satisfied
    assert res.new_value == "v1"
    assert cache[("k",)] == "v1"


def test_get_params_returns_constructor_args(append_only_env):
    """Verify get_params returns the original main_dict and data_cache objects."""
    main, cache, wrapper = append_only_env
    params = wrapper.get_params()
    assert set(params.keys()) == {"data_cache", "main_dict"}
    assert params["main_dict"] is main
    assert params["data_cache"] is cache
    # Roundtrip: reconstructing from get_params produces equivalent wrapper
    wrapper2 = AppendOnlyDictCached(**params)
    assert wrapper2.get_params() == params


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
    with pytest.raises(MutationPolicyError):
        del wrapper["a"]


def test_attempt_to_modify_existing_key_raises(append_only_env):
    main, cache, wrapper = append_only_env

    wrapper[("k1",)] = "v1"
    with pytest.raises(MutationPolicyError):
        wrapper[("k1",)] = "v2"  # append-only


def test_failed_overwrite_preserves_cache(append_only_env):
    """A rejected overwrite must leave both main and cache unchanged."""
    main, cache, wrapper = append_only_env

    wrapper[("k",)] = "original"
    with pytest.raises(MutationPolicyError):
        wrapper[("k",)] = "replacement"

    assert main[("k",)] == "original"
    assert cache[("k",)] == "original"
    assert wrapper[("k",)] == "original"


def test_value_type_validation(tmp_path):
    # Create a typed environment: only ints allowed
    main = FileDirDict(base_dir=str(tmp_path / "typed_main"), append_only=True, serialization_format="json", base_class_for_values=int)
    cache = FileDirDict(base_dir=str(tmp_path / "typed_cache"), append_only=True, serialization_format="json", base_class_for_values=int)
    wrapper = AppendOnlyDictCached(main_dict=main, data_cache=cache)

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
        with pytest.raises(MutationPolicyError):
            del cache[key]  # FileDirDict will raise on append-only; just ignore
    with pytest.raises(KeyError):
        _ = wrapper[key]


# --- Additional thoroughness tests -----------------------------------------

def test_set_item_get_etag_keep_current_noop(append_only_env):
    main, cache, wrapper = append_only_env

    # No prior value; KEEP_CURRENT should be a no-op and return None
    wrapper[("z",)] = KEEP_CURRENT
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


def test_get_item_if_etag_different_absent_key_does_not_cache(append_only_env):
    main, cache, wrapper = append_only_env

    # Ensure key is absent everywhere
    key = ("nope",)
    assert key not in main and key not in cache

    result = wrapper.get_item_if(key, condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)
    assert result.actual_etag is ITEM_NOT_AVAILABLE

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
    from tests.minimum_sleep import min_sleep

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



def test_setdefault_if_insert_populates_cache(append_only_env):
    """Verify setdefault_if delegates to main and mirrors value into cache on insert."""
    main, cache, wrapper = append_only_env

    res = wrapper.setdefault_if(
        ("sdi",), default_value="default_val", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert res.condition_was_satisfied
    assert ("sdi",) in main and ("sdi",) in cache
    assert main[("sdi",)] == cache[("sdi",)] == "default_val"


def test_setdefault_if_existing_key_populates_cache(append_only_env):
    """Verify setdefault_if on existing key caches the returned value."""
    main, cache, wrapper = append_only_env
    main[("sde",)] = "original"
    assert ("sde",) not in cache

    res = wrapper.setdefault_if(
        ("sde",), default_value="ignored", condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)

    assert res.new_value == "original"
    assert cache[("sde",)] == "original"


def test_setdefault_if_absent_condition_fails_no_cache_pollution(append_only_env):
    """Verify setdefault_if with unsatisfied condition leaves caches empty."""
    main, cache, wrapper = append_only_env

    res = wrapper.setdefault_if(
        ("sdx",), default_value="val", condition=ETAG_HAS_CHANGED, expected_etag=ITEM_NOT_AVAILABLE)

    assert not res.condition_was_satisfied
    assert ("sdx",) not in main
    assert ("sdx",) not in cache


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
