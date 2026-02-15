"""Tests that FileDirDict ignores non-data files for len(), keys(), items(), values().

Editor artifacts, OS metadata files, and unexpected directories under
base_dir must not appear as dict entries or inflate len().
"""

from persidict import FileDirDict


def test_len_ignores_junk_files(tmp_path):
    """len() counts only files matching the serialization format extension."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["real"] = "value"

    # Plant junk files and a junk directory
    (tmp_path / "junk.tmp").write_text("garbage")
    (tmp_path / ".DS_Store").write_bytes(b"\x00")
    (tmp_path / "note.txt").write_text("a note")
    junk_dir = tmp_path / "junk_dir"
    junk_dir.mkdir()
    (junk_dir / "nested.tmp").write_text("nested junk")

    assert len(d) == 1


def test_keys_ignores_junk_files(tmp_path):
    """keys() yields only keys from real persisted data files."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="pkl")
    d["alpha"] = 1
    d["beta"] = 2

    (tmp_path / "stray.json").write_text("{}")
    (tmp_path / ".gitkeep").write_text("")
    (tmp_path / "Thumbs.db").write_bytes(b"\x00")

    keys = [k[0] for k in d.keys()]

    assert len(keys) == 2
    assert set(keys) == {"alpha", "beta"}


def test_items_ignores_junk_files(tmp_path):
    """items() does not raise and does not include junk-derived entries."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["only"] = {"data": True}

    (tmp_path / "random.csv").write_text("a,b,c")
    (tmp_path / "backup.json.bak").write_text("{}")
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "deep_junk.txt").write_text("deep")

    items = dict(d.items())

    assert len(items) == 1
    assert items[("only",)] == {"data": True}


def test_values_ignores_junk_files(tmp_path):
    """values() is not affected by non-data files in the base directory."""
    d = FileDirDict(base_dir=str(tmp_path), serialization_format="json")
    d["k1"] = "v1"

    (tmp_path / ".hidden").write_text("secret")
    (tmp_path / "readme.md").write_text("# Hello")

    values = list(d.values())

    assert values == ["v1"]
