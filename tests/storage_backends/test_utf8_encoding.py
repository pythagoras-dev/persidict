"""Verify that FileDirDict text/JSON I/O uses UTF-8 regardless of platform locale.

Non-pickle serialization formats open files in text mode.  Before the
explicit encoding="utf-8" fix, the platform default encoding was used,
which could silently corrupt non-ASCII data on Windows (cp1252) or
non-UTF-8 Linux locales.  These tests pin the correct round-trip
behavior for representative Unicode inputs across every text-based
serialization path.
"""

import pytest
from persidict import FileDirDict


# Representative non-ASCII strings covering Latin accents, CJK, Cyrillic,
# emoji, and mixed scripts.  Each is a realistic value a caller might store.
_UNICODE_SAMPLES = [
    "café résumé naïve",
    "日本語テスト",
    "Кириллица",
    "🎉🚀✅",
    "mixed: Ω≈ç√∫≤≥÷",
    "path/like\u2014value \u00abwith\u00bb \u201cquotes\u201d",
]


@pytest.mark.parametrize("text", _UNICODE_SAMPLES, ids=[
    "latin_accents", "cjk", "cyrillic", "emoji", "math_symbols", "mixed_punctuation",
])
@pytest.mark.parametrize("fmt", ["json", "txt"])
def test_non_ascii_roundtrip(tmp_path, fmt, text):
    """Non-ASCII text must survive a write→read cycle for every text format."""
    kwargs = {"base_dir": str(tmp_path / "d"), "serialization_format": fmt}
    if fmt not in ("pkl", "json"):
        kwargs["base_class_for_values"] = str

    d = FileDirDict(**kwargs)
    d["key"] = text
    assert d["key"] == text


@pytest.mark.parametrize("fmt", ["json", "txt"])
def test_non_ascii_roundtrip_via_iteration(tmp_path, fmt):
    """Values with non-ASCII content must round-trip through .items() iteration."""
    kwargs = {"base_dir": str(tmp_path / "d"), "serialization_format": fmt}
    if fmt not in ("pkl", "json"):
        kwargs["base_class_for_values"] = str

    d = FileDirDict(**kwargs)
    d["k1"] = "données"
    d["k2"] = "数据"

    retrieved = {k.strings[0]: v for k, v in d.items()}
    assert retrieved["k1"] == "données"
    assert retrieved["k2"] == "数据"


def test_json_non_ascii_nested_object(tmp_path):
    """JSON format must preserve non-ASCII text inside nested structures."""
    d = FileDirDict(base_dir=str(tmp_path / "d"), serialization_format="json")
    value = {"greeting": "こんにちは", "items": ["données", "Ω"]}
    d["nested"] = value
    assert d["nested"] == value
