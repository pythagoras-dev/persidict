from pathlib import Path

from persidict import FileDirDict, KEEP_CURRENT, DELETE_CURRENT, WriteOnceDict

import pytest

def test_first_entry_dict_no_checks(tmpdir):
    d = FileDirDict(base_dir=tmpdir, append_only=True)
    fed = WriteOnceDict(wrapped_dict=d, p_consistency_checks=None)
    for i in range(1,100):
        key = "a_"+str(i)
        value = i*i
        fed[key] = value
        assert fed[key] == value
        fed[key] = 2
        assert fed[key] == value
        fed[key] = value
        assert fed[key] == value
        assert len(fed) == i

def test_first_entry_dict_pchecks_zero(tmpdir):
    d = FileDirDict(base_dir=tmpdir, append_only=True)
    fed = WriteOnceDict(wrapped_dict=d, p_consistency_checks=0)
    for i in range(1,100):
        key = "a_"+str(i)
        value = i*i
        fed[key] = value
        assert fed[key] == value
        fed[key] = 2
        assert fed[key] == value
        fed[key] = value
        assert fed[key] == value
        assert len(fed) == i

def test_first_entry_dict_pchecks_one(tmpdir):
    d = FileDirDict(base_dir=tmpdir, append_only=True)
    fed = WriteOnceDict(wrapped_dict=d, p_consistency_checks=1)
    for i in range(1,100):
        key = "a_"+str(i)
        value = i*i*i
        fed[key] = value
        assert fed[key] == value
        with pytest.raises(ValueError):
            fed[key] = 3
        assert fed[key] == value
        fed[key] = value
        assert fed[key] == value
        with pytest.raises(ValueError):
            fed[key] = -i
        assert len(fed) == i


def test_firs_entry_dict_wrong_init_params(tmpdir):
    with pytest.raises(Exception):
        _ = WriteOnceDict(wrapped_dict={}, p_consistency_checks=None)

    with pytest.raises(Exception):
        _ = WriteOnceDict(
            wrapped_dict=FileDirDict(base_dir=tmpdir, append_only=True)
            , p_consistency_checks=1.2)

    with pytest.raises(Exception):
        _ = WriteOnceDict(
            wrapped_dict=FileDirDict(base_dir=tmpdir, append_only=True)
            , p_consistency_checks=-0.1)

    with pytest.raises(Exception):
        _ = WriteOnceDict(
            wrapped_dict=FileDirDict(base_dir=tmpdir, append_only=False))


def test_write_once_dict_default_wrapped_dict_uses_append_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = WriteOnceDict()

    assert d.append_only is True
    assert d.p_consistency_checks == 0.0

    params = d.get_params()
    assert params["p_consistency_checks"] == d.p_consistency_checks
    assert params["wrapped_dict"].base_dir == d.base_dir

    base_dir = Path(d.base_dir)
    assert base_dir.is_dir()
    assert base_dir.name == "__file_dir_dict__"


def test_write_once_dict_keep_current_keeps_probability(tmp_path):
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=0.25,
    )
    d.p_consistency_checks = KEEP_CURRENT

    assert d.p_consistency_checks == 0.25


def test_write_once_dict_consistency_counters_increment(tmp_path):
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=1,
    )
    d["k"] = {"a": 1}
    d["k"] = {"a": 1}

    assert d.consistency_checks_attempted == 1
    assert d.consistency_checks_passed == 1
    assert d.consistency_checks_failed == 0


def test_write_once_dict_delete_raises_type_error(tmp_path):
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=0,
    )
    d["k"] = "value"

    with pytest.raises(TypeError):
        del d["k"]


def test_write_once_dict_consistency_check_uses_prefetched_value(tmp_path):
    """Verify that a duplicate write with p_consistency_checks=1 performs
    exactly one backend read (inside setdefault_if) and none extra for the
    consistency check itself."""
    wrapped = FileDirDict(base_dir=tmp_path, append_only=True)
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=1)

    d["k"] = "hello"

    getitem_calls = 0
    original_getitem = type(wrapped).__getitem__

    def counting_getitem(self, key):
        nonlocal getitem_calls
        getitem_calls += 1
        return original_getitem(self, key)

    type(wrapped).__getitem__ = counting_getitem
    try:
        d["k"] = "hello"
    finally:
        type(wrapped).__getitem__ = original_getitem

    assert d.consistency_checks_attempted == 1
    assert d.consistency_checks_passed == 1
    # Exactly 1 read: setdefault_if retrieves existing value.
    # The consistency check must reuse that value, not read again.
    assert getitem_calls == 1


def test_write_once_dict_low_p_skips_read_when_check_not_triggered(tmp_path):
    """When p < 1 and the random check does not fire, no backend read
    should occur on a duplicate write (value is not fetched at all)."""
    import random as stdlib_random

    wrapped = FileDirDict(base_dir=tmp_path, append_only=True)
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=0.5)
    d["k"] = "hello"

    getitem_calls = 0
    original_getitem = type(wrapped).__getitem__

    def counting_getitem(self, key):
        nonlocal getitem_calls
        getitem_calls += 1
        return original_getitem(self, key)

    # Seed so random.random() > 0.5 → check does not fire
    stdlib_random.seed(0)  # random.random() ≈ 0.844 > 0.5
    type(wrapped).__getitem__ = counting_getitem
    try:
        d["k"] = "hello"
    finally:
        type(wrapped).__getitem__ = original_getitem

    assert d.consistency_checks_attempted == 0
    assert getitem_calls == 0


def test_write_once_dict_low_p_reads_separately_when_check_triggered(tmp_path):
    """When p < 1 and the random check fires, a separate backend read is
    performed to fetch the stored value for comparison."""
    import random as stdlib_random

    wrapped = FileDirDict(base_dir=tmp_path, append_only=True)
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=0.5)
    d["k"] = "hello"

    getitem_calls = 0
    original_getitem = type(wrapped).__getitem__

    def counting_getitem(self, key):
        nonlocal getitem_calls
        getitem_calls += 1
        return original_getitem(self, key)

    # Seed so random.random() < 0.5 → check fires
    stdlib_random.seed(4)  # random.random() ≈ 0.236 < 0.5
    type(wrapped).__getitem__ = counting_getitem
    try:
        d["k"] = "hello"
    finally:
        type(wrapped).__getitem__ = original_getitem

    assert d.consistency_checks_attempted == 1
    assert d.consistency_checks_passed == 1
    # Exactly 1 read: the separate __getitem__ for the consistency check
    assert getitem_calls == 1


def test_write_once_dict_p_zero_no_reads_on_duplicate(tmp_path):
    """When p=0, duplicate writes must not read the stored value at all."""
    wrapped = FileDirDict(base_dir=tmp_path, append_only=True)
    d = WriteOnceDict(wrapped_dict=wrapped, p_consistency_checks=0)
    d["k"] = "hello"

    getitem_calls = 0
    original_getitem = type(wrapped).__getitem__

    def counting_getitem(self, key):
        nonlocal getitem_calls
        getitem_calls += 1
        return original_getitem(self, key)

    type(wrapped).__getitem__ = counting_getitem
    try:
        for _ in range(10):
            d["k"] = "hello"
    finally:
        type(wrapped).__getitem__ = original_getitem

    assert getitem_calls == 0
    assert d.consistency_checks_attempted == 0


def test_write_once_dict_get_subdict_preserves_settings(tmp_path):
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=0.5,
    )
    d[("parent", "child")] = "value"

    sub = d.get_subdict("parent")

    assert isinstance(sub, WriteOnceDict)
    assert sub.p_consistency_checks == d.p_consistency_checks
    assert sub["child"] == "value"


def test_write_once_dict_keep_current_noop_on_existing_key(tmp_path):
    """KEEP_CURRENT via __setitem__ preserves the stored value."""
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=1,
    )
    d["k"] = "original"
    d["k"] = KEEP_CURRENT

    assert d["k"] == "original"
    assert len(d) == 1


def test_write_once_dict_keep_current_noop_on_missing_key(tmp_path):
    """KEEP_CURRENT on a missing key is a silent no-op."""
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=0,
    )
    d["k"] = KEEP_CURRENT

    assert "k" not in d
    assert len(d) == 0


def test_write_once_dict_delete_current_raises(tmp_path):
    """DELETE_CURRENT raises KeyError because WriteOnceDict is append-only."""
    d = WriteOnceDict(
        wrapped_dict=FileDirDict(base_dir=tmp_path, append_only=True),
        p_consistency_checks=0,
    )
    d["k"] = "value"

    with pytest.raises(KeyError):
        d["k"] = DELETE_CURRENT

    assert d["k"] == "value"
