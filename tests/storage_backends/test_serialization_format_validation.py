"""Verify that PersiDict.__init__ rejects exotic serialization formats for non-string values.

The combination of a custom serialization_format (not 'pkl' or 'json') with
base_class_for_values that is None or a non-str type must be rejected at
construction time, since only pkl/json support arbitrary object serialization.
The valid path — custom format with base_class_for_values=str — must work.
"""

import pytest
from persidict import FileDirDict, LocalDict


@pytest.mark.parametrize("DictClass", [FileDirDict, LocalDict])
def test_exotic_format_with_no_base_class_rejected(tmp_path, DictClass):
    """Custom format + base_class_for_values=None must raise ValueError."""
    kwargs = {"serialization_format": "txt"}
    if DictClass is FileDirDict:
        kwargs["base_dir"] = str(tmp_path / "d")
    with pytest.raises(ValueError):
        DictClass(**kwargs)


@pytest.mark.parametrize("DictClass", [FileDirDict, LocalDict])
def test_exotic_format_with_non_str_base_class_rejected(tmp_path, DictClass):
    """Custom format + base_class_for_values=int must raise ValueError."""
    kwargs = {"serialization_format": "csv", "base_class_for_values": int}
    if DictClass is FileDirDict:
        kwargs["base_dir"] = str(tmp_path / "d")
    with pytest.raises(ValueError):
        DictClass(**kwargs)


@pytest.mark.parametrize("DictClass", [FileDirDict, LocalDict])
def test_exotic_format_with_str_base_class_accepted(tmp_path, DictClass):
    """Custom format + base_class_for_values=str is valid and must work."""
    kwargs = {"serialization_format": "txt", "base_class_for_values": str}
    if DictClass is FileDirDict:
        kwargs["base_dir"] = str(tmp_path / "d")
    d = DictClass(**kwargs)

    d["hello"] = "world"
    assert d["hello"] == "world"


@pytest.mark.parametrize("fmt", ["pkl", "json"])
def test_standard_formats_accepted_without_base_class(tmp_path, fmt):
    """pkl and json formats must work with base_class_for_values=None."""
    d = FileDirDict(base_dir=str(tmp_path / "d"), serialization_format=fmt)
    d["key"] = 42
    assert d["key"] == 42
