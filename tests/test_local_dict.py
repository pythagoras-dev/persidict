import time
import pytest

from persidict import LocalDict
from persidict.safe_str_tuple import SafeStrTuple
from persidict.singletons import KEEP_CURRENT, DELETE_CURRENT

# LocalDict uses time.time(), a small sleep ensures distinct timestamps
MIN_SLEEP = 0.02


def make_ld(*, file_type: str = "pkl", base_dir=None, **kwargs):
    # base_dir is accepted for API compatibility with other backends/fixtures
    # but LocalDict is purely in-memory and ignores it.
    return LocalDict(file_type=file_type, **kwargs)


def test_init_and_get_params_defaults(tmp_path):
    ld = make_ld(base_dir=tmp_path)
    params = ld.get_params()
    assert params["file_type"] == "pkl"
    assert params["immutable_items"] is False
    assert params["base_class_for_values"] is None
    # backend object identity is included
    assert params["backend"] is not None
    assert params["backend"] is ld.get_params()["backend"]


def test_file_type_validation_and_repr():
    # repr contains class name and params (and does not raise)
    ld = make_ld(file_type="json")
    s = repr(ld)
    assert "LocalDict(" in s
    assert "file_type='json'" in s


def test_basic_crud_and_len_contains_timestamp():
    ld = make_ld()
    assert len(ld) == 0
    with pytest.raises(KeyError):
        _ = ld["missing"]
    k1 = ("a", "b")
    k2 = SafeStrTuple(("a", "c"))
    ld[k1] = 1
    t1 = ld.timestamp(k1)
    assert isinstance(t1, float)
    time.sleep(MIN_SLEEP)
    ld[k2] = 2
    t2 = ld.timestamp(k2)
    assert t2 > t1
    assert k1 in ld and SafeStrTuple(k1) in ld
    assert "a" not in ld  # Non-empty hierarchical keys only
    assert len(ld) == 2
    assert ld[k1] == 1 and ld[k2] == 2

    # delete and missing timestamp
    del ld[k1]
    assert len(ld) == 1
    with pytest.raises(KeyError):
        ld.timestamp(k1)


def test_delete_if_exists_and_clear():
    ld = make_ld()
    ld[("x",)] = 10
    assert ld.delete_if_exists(("x",)) is True
    assert ld.delete_if_exists(("x",)) is False

    # clear removes everything
    for i in range(3):
        ld[("k", str(i))] = i
    assert len(ld) == 3
    ld.clear()
    assert len(ld) == 0
    # clearing an empty dict is fine
    ld.clear()


def test_immutable_items_prohibits_overwrite_and_delete():
    ld = make_ld(immutable_items=True)
    k = ("root", "leaf")
    ld[k] = 5
    with pytest.raises(KeyError):
        ld[k] = 6
    with pytest.raises(KeyError):
        del ld[k]
    with pytest.raises(KeyError):
        ld.clear()
    with pytest.raises(KeyError):
        ld.delete_if_exists(k)


def test_type_enforcement_base_class_for_values():
    class Base:
        pass
    class Child(Base):
        pass
    ld = make_ld(base_class_for_values=Base)
    ld[("ok",)] = Child()
    with pytest.raises(TypeError):
        ld[("bad",)] = object()

    # For string-only base, non-json/pkl file_type must have raised in ctor;
    # confirm that json works for strings
    ld_str = make_ld(file_type="json", base_class_for_values=str)
    ld_str[("s",)] = "hello"


def test_no_nested_persidict_values():
    ld = make_ld()
    other = make_ld()
    with pytest.raises(TypeError):
        ld[("nested",)] = other


def test_setdefault_and_equality():
    ld1 = make_ld(file_type="json")
    ld2 = make_ld(file_type="json", backend=ld1.get_params()["backend"])  # same backend
    # setdefault returns default when absent
    v = ld1.setdefault(("a",), {"x": 1})
    assert v == {"x": 1}
    # now get current
    assert ld1.setdefault(("a",), {"y": 2}) == {"x": 1}

    # equality for PersiDict instances compares params, not content
    assert (ld1 == ld2)  # same params (same backend, file_type, etc.)
    ld3 = make_ld(file_type="json")
    assert not (ld1 == ld3)  # different backend


def test_iterations_and_timestamps_variants():
    ld = make_ld()
    data = {
        ("r", "a"): 1,
        ("r", "b"): 2,
        ("r", "c"): 3,
    }
    for k, v in data.items():
        ld[k] = v
        time.sleep(MIN_SLEEP)
    # keys()
    keys = list(ld.keys())
    assert all(isinstance(k, SafeStrTuple) for k in keys)
    # values()
    vals = list(ld.values())
    assert sorted(vals) == [1, 2, 3]
    # items()
    items = list(ld.items())
    assert {tuple(k): v for k, v in items} == data
    # keys_and_timestamps
    kat = list(ld.keys_and_timestamps())
    for k, ts in kat:
        assert isinstance(k, SafeStrTuple) and isinstance(ts, float)
    # values_and_timestamps
    vat = list(ld.values_and_timestamps())
    for v, ts in vat:
        assert v in {1, 2, 3} and isinstance(ts, float)
    # items_and_timestamps
    iat = list(ld.items_and_timestamps())
    for k, v, ts in iat:
        assert tuple(k) in data and data[tuple(k)] == v and isinstance(ts, float)


def test_newest_oldest_helpers():
    ld = make_ld()
    ld[("t", "1")] = "a"
    t1 = ld.timestamp(("t", "1"))
    time.sleep(MIN_SLEEP)
    ld[("t", "2")] = "b"
    t2 = ld.timestamp(("t", "2"))
    time.sleep(MIN_SLEEP)
    ld[("t", "3")] = "c"
    t3 = ld.timestamp(("t", "3"))

    # newest
    nk = ld.newest_keys()
    nv = ld.newest_values()
    assert tuple(nk[0]) == ("t", "3") and nv[0] == "c"
    # oldest
    ok = ld.oldest_keys()
    ov = ld.oldest_values()
    assert tuple(ok[0]) == ("t", "1") and ov[0] == "a"

    # with limits
    assert [tuple(k) for k in ld.newest_keys(2)] == [("t", "3"), ("t", "2")]
    assert [tuple(k) for k in ld.oldest_keys(2)] == [("t", "1"), ("t", "2")]


def test_random_key_behavior():
    ld = make_ld()
    # Empty dict returns None
    assert ld.random_key() is None
    # Single key: always that key
    ld[("a", "b")] = 1
    rk = ld.random_key()
    assert rk == SafeStrTuple(("a", "b"))
    # Multiple keys: returns only existing keys and never None
    keys = [("a", "b"), ("a", "c"), ("x", "y")]
    for i, k in enumerate(keys[1:], start=2):
        ld[k] = i
    seen = set()
    for _ in range(20):
        k = ld.random_key()
        assert k is not None
        assert tuple(k) in keys
        seen.add(tuple(k))
    assert seen.issubset(set(keys))


@pytest.mark.parametrize("file_type", ["pkl", "json"])
def test_subdict_and_isolation_with_parent(file_type):
    ld = make_ld(file_type=file_type)
    ld[("root", "x")] = 1
    ld[("root", "y")] = 2
    ld[("other", "z")] = 3

    sub = ld.get_subdict(("root",))
    assert isinstance(sub, LocalDict)
    assert len(sub) == 2
    assert set(map(tuple, sub.keys())) == {("x",), ("y",)}

    # Write via subdict is visible to parent
    sub[("w",)] = 4
    assert ld[("root", "w")] == 4

    # And vice versa
    ld[("root", "v")] = 5
    assert sub[("v",)] == 5

    # Deleting at parent prunes subdict view immediately
    del ld[("root", "x")]
    assert ("x",) not in set(map(tuple, sub.keys()))

    # Deleting at subdict prunes parent view
    del sub[("y",)]
    assert ("root", "y") not in set(map(tuple, ld.keys()))


def test_file_type_buckets_and_shared_backend():
    base = make_ld()
    base[("k", "1")] = 1
    backend = base.get_params()["backend"]

    # Different file_type sees different bucket on same backend
    ld_json = make_ld(file_type="json", backend=backend)
    assert len(ld_json) == 0
    with pytest.raises(KeyError):
        _ = ld_json[("k", "1")]

    # Same file_type shares content
    ld_same = make_ld(file_type=base.file_type, backend=backend)
    assert len(ld_same) == 1
    assert ld_same[("k", "1")] == 1


def test_pruning_removes_empty_subtrees_from_subdicts():
    ld = make_ld()
    # create nested entries
    ld[("p", "q", "r")] = 1
    ld[("p", "q", "s")] = 2
    # Verify that at least one key starts with the prefix ("p",)
    assert any(tuple(k)[:1] == ("p",) for k in ld.keys())
    # delete all under that subtree
    del ld[("p", "q", "r")]
    del ld[("p", "q", "s")]
    # After deletions and a prune cycle, there should be no keys under ("p",)
    sub = ld.get_subdict(("p",))
    assert len(sub) == 0


def test_jokers_keep_and_delete_current():
    ld = make_ld()
    k = ("j", "k")
    # KEEP_CURRENT on missing is a no-op and should not raise; key remains absent
    ld[k] = KEEP_CURRENT
    assert k not in ld
    assert len(ld) == 0
    # Normal set
    ld[k] = 7
    old_ts = ld.timestamp(k)
    time.sleep(MIN_SLEEP)
    # KEEP_CURRENT keeps value and should not change timestamp
    ld[k] = KEEP_CURRENT
    assert ld[k] == 7
    assert ld.timestamp(k) == old_ts

    # DELETE_CURRENT deletes
    ld[k] = DELETE_CURRENT
    assert k not in ld
    with pytest.raises(KeyError):
        _ = ld[k]


@pytest.mark.parametrize("bad_key", [
    (),  # empty tuple key
    ("",),  # empty segment
    (None,),  # type error inside tuple
    ("ok", 1),  # mixed type in tuple
    123,  # non-sequence / invalid key type
    b"bytes",  # bytes not accepted
    (" ",),  # whitespace-only segment (unsafe)
    ("a", "", "b"),  # nested empty segment
])
def test_invalid_keys(bad_key):
    ld = make_ld()
    with pytest.raises((TypeError, ValueError, KeyError)):
        ld[bad_key] = 1



def test_keys_values_items_shapes_and_types():
    ld = make_ld()
    ld[("a", "1")] = {"x": 1}
    ld[("a", "2")] = {"y": 2}

    for k in ld.keys():
        assert isinstance(k, SafeStrTuple)
    for v in ld.values():
        assert isinstance(v, dict)
    for k, v in ld.items():
        assert isinstance(k, SafeStrTuple) and isinstance(v, dict)



def test_delete_current_on_missing_is_noop():
    ld = make_ld()
    k = ("x", "y")
    ld[k] = DELETE_CURRENT  # should not raise
    assert k not in ld

@pytest.mark.parametrize("file_type", ["pkl", "json"])
def test_roundtrip_basic_by_file_type(file_type):
    ld = make_ld(file_type=file_type)
    k = ("a", "b")
    v = {"x": 1}
    ld[k] = v
    assert ld[k] == v
    # For pkl, also verify custom class instance round-trip
    if file_type == "pkl":
        class MyObj:
            def __init__(self, a):
                self.a = a
        obj = MyObj(42)
        kk = ("obj",)
        ld[kk] = obj
        got = ld[kk]
        assert isinstance(got, MyObj) and got.a == 42

@pytest.mark.parametrize("bad", ["", "bad/ty%pe", "../etc", "\x00", 123, None])
def test_file_type_validation(bad):
    with pytest.raises((TypeError, ValueError)):
        make_ld(file_type=bad)  # type: ignore[arg-type]


def test_subdict_deep_prefix_isolation():
    ld = make_ld()
    ld[("root", "a", "1")] = 1
    sub = ld.get_subdict(("root", "a"))
    assert list(map(tuple, sub.keys())) == [("1",)]
    # Set in subdict
    sub[("2",)] = 2
    assert ld[("root", "a", "2")] == 2
    # Delete in subdict
    del sub[("1",)]
    assert ("root", "a", "1") not in map(tuple, ld.keys())



def test_setdefault_mutation_persists():
    ld = make_ld(file_type="json")
    d = {"x": 1}
    got = ld.setdefault(("a",), d)
    got["y"] = 2
    assert ld[("a",)] == {"x": 1, "y": 2}











def test_key_normalization_equivalence():
    ld = make_ld()
    t = ("a", "b")
    sst = SafeStrTuple(t)
    ld[t] = 1
    assert ld[sst] == 1
    ld[sst] = 2
    assert ld[t] == 2


def test_timestamp_overwrite_vs_keep_current(monkeypatch):
    ld = make_ld()
    # First write at t=1000
    monkeypatch.setattr("persidict.local_dict.time.time", lambda: 1000.0)
    k = ("t", "k")
    ld[k] = 1
    t1 = ld.timestamp(k)
    assert t1 == 1000.0
    # KEEP_CURRENT does not update timestamp
    monkeypatch.setattr("persidict.local_dict.time.time", lambda: 1001.0)
    ld[k] = KEEP_CURRENT
    assert ld.timestamp(k) == 1000.0
    # Normal overwrite updates timestamp
    monkeypatch.setattr("persidict.local_dict.time.time", lambda: 1002.0)
    ld[k] = 2
    assert ld.timestamp(k) == 1002.0


@pytest.mark.parametrize("file_type", ["pkl", "json"])
def test_delete_if_exists_immutable_raises(file_type):
    ld = make_ld(file_type=file_type, immutable_items=True)
    with pytest.raises(KeyError):
        ld.delete_if_exists(("a",))
