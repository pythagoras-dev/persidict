"""Tests for path traversal prevention.

Verifies that '.' and '..' are rejected as key components in SafeStrTuple,
and that FileDirDict's _build_full_path refuses to resolve paths outside
base_dir (defense-in-depth).
"""

import pytest

from persidict import SafeStrTuple, NonEmptySafeStrTuple, FileDirDict

pytestmark = pytest.mark.smoke


# --- SafeStrTuple rejection of '.' and '..' ---


@pytest.mark.parametrize("args", [
    (".",),
    ("..",),
    ("a", ".", "b"),
    ("a", "..", "b"),
    (["a", "."],),
    (["a", ".."],),
])
def test_dot_and_dotdot_rejected(args):
    """SafeStrTuple rejects '.' and '..' in any position and nesting."""
    with pytest.raises(ValueError):
        SafeStrTuple(*args)


def test_non_empty_variant_rejects_dotdot():
    with pytest.raises(ValueError):
        NonEmptySafeStrTuple("..")


def test_dot_containing_strings_still_allowed():
    """Strings that merely contain dots (e.g. '.hidden', 'a.b', '...')
    are not filesystem-special and must be accepted."""
    s = SafeStrTuple(".hidden", "..name", "a.b", "x..y", "...")
    assert len(s) == 5


# --- FileDirDict containment check ---


@pytest.mark.parametrize("digest_len", [0, 4])
def test_traversal_blocked_at_safestrtuple_level(tmp_path, digest_len):
    """'..' is rejected before it ever reaches the filesystem layer."""
    d = FileDirDict(base_dir=str(tmp_path / "store"), digest_len=digest_len)
    with pytest.raises(ValueError):
        d["..", "escape"] = "payload"


def test_containment_check_defense_in_depth(tmp_path, monkeypatch):
    """The _build_full_path containment guard catches traversal even when
    SafeStrTuple validation is bypassed (defense-in-depth)."""
    import persidict.file_dir_dict as fdd

    d = FileDirDict(base_dir=str(tmp_path / "store"), digest_len=0)

    # Bypass signing so the forged key reaches os.path.join unchanged.
    monkeypatch.setattr(fdd, "sign_safe_str_tuple", lambda k, dl: k)

    # Forge a SafeStrTuple with '..' by writing directly to the .strings
    # attribute, bypassing __init__ validation.
    bad_key = SafeStrTuple.__new__(SafeStrTuple)
    bad_key.strings = ("..", "..", "etc", "passwd")

    with pytest.raises(ValueError, match="outside base_dir"):
        d._build_full_path(bad_key)


def test_normal_keys_unaffected_with_digest_len_zero(tmp_path):
    """Regular keys still round-trip correctly when digest_len=0."""
    d = FileDirDict(
        base_dir=str(tmp_path / "store"),
        digest_len=0,
        serialization_format="json",
    )
    d["hello"] = "world"
    assert d["hello"] == "world"

    d["a", "b", "c"] = "deep"
    assert d["a", "b", "c"] == "deep"


def test_empty_prefix_subdict_allowed(tmp_path):
    """get_subdict with an empty prefix returns an equivalent dict
    rooted at the same base_dir — the containment check must not
    reject the base_dir itself."""
    d = FileDirDict(
        base_dir=str(tmp_path / "store"),
        digest_len=0,
        serialization_format="json",
    )
    d["x"] = "val"

    sub = d.get_subdict([])
    assert "x" in sub
    assert sub["x"] == "val"
