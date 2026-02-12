"""Tests for read-side base_class_for_values enforcement.

Write-side validation (_validate_value) rejects bad types at store time.
Read-side validation (_validate_returned_value) catches type mismatches
when a value was written without a constraint and later read through a
handle that has base_class_for_values set.

Both LocalDict and FileDirDict are exercised because each has its own
_get_value / _get_value_and_etag implementation.
"""

import pytest

from persidict import FileDirDict, LocalDict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
)


# -- LocalDict ---------------------------------------------------------------

def test_localdict_getitem_rejects_mismatched_type():
    """Reading a non-int value through an int-constrained handle raises."""
    unconstrained = LocalDict(serialization_format="pkl")
    unconstrained["k"] = "not-an-int"

    constrained = LocalDict(
        backend=unconstrained.get_params()["backend"],
        serialization_format="pkl",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError, match="int"):
        _ = constrained["k"]


def test_localdict_get_item_if_rejects_mismatched_type():
    """get_item_if exercises _get_value_and_etag and also validates."""
    unconstrained = LocalDict(serialization_format="pkl")
    unconstrained["k"] = "not-an-int"
    etag = unconstrained.etag("k")

    constrained = LocalDict(
        backend=unconstrained.get_params()["backend"],
        serialization_format="pkl",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError, match="int"):
        constrained.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)


def test_localdict_getitem_accepts_matching_type():
    """A value whose type matches base_class_for_values is returned normally."""
    unconstrained = LocalDict(serialization_format="pkl")
    unconstrained["k"] = 42

    constrained = LocalDict(
        backend=unconstrained.get_params()["backend"],
        serialization_format="pkl",
        base_class_for_values=int,
    )

    assert constrained["k"] == 42


def test_localdict_accepts_subclass():
    """A value that is a subclass of base_class_for_values passes validation."""
    unconstrained = LocalDict(serialization_format="pkl")
    unconstrained["k"] = True  # bool is a subclass of int

    constrained = LocalDict(
        backend=unconstrained.get_params()["backend"],
        serialization_format="pkl",
        base_class_for_values=int,
    )

    assert constrained["k"] is True


# -- FileDirDict -------------------------------------------------------------

def test_filedirdict_getitem_rejects_mismatched_type(tmp_path):
    """Reading a non-int value through an int-constrained handle raises."""
    unconstrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
    )
    unconstrained["k"] = "not-an-int"

    constrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError, match="int"):
        _ = constrained["k"]


def test_filedirdict_get_item_if_rejects_mismatched_type(tmp_path):
    """get_item_if exercises _get_value_and_etag and also validates."""
    unconstrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
    )
    unconstrained["k"] = "not-an-int"
    etag = unconstrained.etag("k")

    constrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
        base_class_for_values=int,
    )

    with pytest.raises(TypeError, match="int"):
        constrained.get_item_if("k", condition=ETAG_IS_THE_SAME, expected_etag=etag)


def test_filedirdict_getitem_accepts_matching_type(tmp_path):
    """A value whose type matches base_class_for_values is returned normally."""
    unconstrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
    )
    unconstrained["k"] = 42

    constrained = FileDirDict(
        base_dir=str(tmp_path), serialization_format="json",
        base_class_for_values=int,
    )

    assert constrained["k"] == 42
