from pathlib import Path

from persidict import FileDirDict, KEEP_CURRENT, WriteOnceDict

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
