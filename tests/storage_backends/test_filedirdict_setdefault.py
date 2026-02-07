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


def test_filedirdict_rejects_negative_digest_len(tmp_path):
    with pytest.raises(ValueError):
        FileDirDict(base_dir=tmp_path, digest_len=-1)


def test_filedirdict_rejects_base_dir_file(tmp_path):
    base_path = tmp_path / "not_a_dir"
    base_path.write_text("data")

    with pytest.raises(ValueError):
        FileDirDict(base_dir=base_path)
