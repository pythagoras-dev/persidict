"""Static typing assertions validated by mypy/pyright (not executed at runtime)."""

from typing import Any, TYPE_CHECKING, assert_type

if TYPE_CHECKING:
    from persidict import (
        EmptyDict,
        FileDirDict,
        LocalDict,
        NonEmptySafeStrTuple,
    )

    # Parameterized usage - typed dictionaries
    int_dict: FileDirDict[int]
    assert_type(int_dict["key"], int)
    assert_type(int_dict.get("key"), int | None)
    assert_type(int_dict.get("key", 0), int)
    assert_type(int_dict.setdefault("missing", 0), int)
    assert_type(int_dict.pop("missing"), int)
    assert_type(int_dict.pop("missing", 0), int)
    assert_type(int_dict.timestamp("key"), float)

    for key in int_dict.keys():
        assert_type(key, NonEmptySafeStrTuple)
    for value in int_dict.values():
        assert_type(value, int)
    for key, value in int_dict.items():
        assert_type(key, NonEmptySafeStrTuple)
        assert_type(value, int)

    str_dict: LocalDict[str]
    assert_type(str_dict["another_key"], str)
    assert_type(str_dict.get("another_key"), str | None)
    assert_type(str_dict.get("another_key", "default"), str)
    assert_type(str_dict.setdefault("missing", "fallback"), str)
    assert_type(str_dict.pop("missing"), str)
    assert_type(str_dict.pop("missing", "fallback"), str)
    assert_type(str_dict.timestamp("another_key"), float)

    for key in str_dict.keys():
        assert_type(key, NonEmptySafeStrTuple)
    for value in str_dict.values():
        assert_type(value, str)
    for key, value in str_dict.items():
        assert_type(key, NonEmptySafeStrTuple)
        assert_type(value, str)

    # Unparameterized usage (backward compatible)
    any_dict: FileDirDict
    assert_type(any_dict["key"], Any)
    assert_type(any_dict.get("key"), Any)
    assert_type(any_dict.pop("key", None), Any)

    # EmptyDict with type parameter
    empty: EmptyDict[float]
    assert_type(empty.get("key"), float | None)
    assert_type(empty.get("key", 1.0), float)
    assert_type(empty.setdefault("key", 1.0), float)
    assert_type(empty.pop("key", 1.0), float)
    assert_type(empty.timestamp("key"), float)


def test_runtime_imports() -> None:
    """Ensure referenced typing targets exist at runtime."""

    from persidict import EmptyDict, FileDirDict, LocalDict

    assert EmptyDict is not None
    assert FileDirDict is not None
    assert LocalDict is not None
