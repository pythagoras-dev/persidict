import pytest

from persidict import FileDirDict


def test_filedirdict_setdefault_existing_key_ignores_invalid_default_type(tmp_path):
    d = FileDirDict(
        base_dir=tmp_path,
        serialization_format="json",
        base_class_for_values=int,
    )
    d["k"] = 1

    result = d.setdefault("k", "bad")

    assert result == 1
    assert d["k"] == 1


def test_filedirdict_setdefault_missing_key_rejects_invalid_default_type(tmp_path):
    d = FileDirDict(
        base_dir=tmp_path,
        serialization_format="json",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError):
        d.setdefault("missing", "bad")
