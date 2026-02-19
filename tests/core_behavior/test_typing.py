"""Static typing assertions validated by mypy/pyright (not executed at runtime)."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, assert_type, cast

    from persidict import (
        ConditionalOperationResult,
        DeleteCurrentFlag,
        EmptyDict,
        ETAG_HAS_CHANGED,
        FileDirDict,
        ITEM_NOT_AVAILABLE,
        ItemNotAvailableFlag,
        KeepCurrentFlag,
        LocalDict,
        NonEmptySafeStrTuple,
        OperationResult,
    )

    # Parameterized usage - typed dictionaries
    int_dict = cast(FileDirDict[int], None)
    assert_type(int_dict["key"], int)
    assert_type(int_dict.get("key"), int | None)
    assert_type(int_dict.get("key", 0), int)
    assert_type(int_dict.setdefault("missing", 0), int)
    assert_type(int_dict.pop("missing"), int)
    assert_type(int_dict.pop("missing", 0), int)
    pi_key, pi_val = int_dict.popitem()
    assert_type(pi_key, NonEmptySafeStrTuple)
    assert_type(pi_val, int)
    assert_type(int_dict.timestamp("key"), float)

    for key in int_dict.keys():
        assert_type(key, NonEmptySafeStrTuple)
    for value in int_dict.values():
        assert_type(value, int)
    for key, value in int_dict.items():
        assert_type(key, NonEmptySafeStrTuple)
        assert_type(value, int)

    str_dict = cast(LocalDict[str], None)
    assert_type(str_dict["another_key"], str)
    assert_type(str_dict.get("another_key"), str | None)
    assert_type(str_dict.get("another_key", "default"), str)
    assert_type(str_dict.setdefault("missing", "fallback"), str)
    assert_type(str_dict.pop("missing"), str)
    assert_type(str_dict.pop("missing", "fallback"), str)
    spi_key, spi_val = str_dict.popitem()
    assert_type(spi_key, NonEmptySafeStrTuple)
    assert_type(spi_val, str)
    assert_type(str_dict.timestamp("another_key"), float)

    for key in str_dict.keys():
        assert_type(key, NonEmptySafeStrTuple)
    for value in str_dict.values():
        assert_type(value, str)
    for key, value in str_dict.items():
        assert_type(key, NonEmptySafeStrTuple)
        assert_type(value, str)

    # Conditional / ETag operations return generic result types
    assert_type(
        int_dict.get_with_etag("key"),
        ConditionalOperationResult[int])
    assert_type(
        int_dict.get_item_if(
            "key", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE),
        ConditionalOperationResult[int])
    assert_type(
        int_dict.set_item_if(
            "key", value=42, condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE),
        ConditionalOperationResult[int])
    assert_type(
        int_dict.setdefault_if(
            "key", default_value=0, condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE),
        ConditionalOperationResult[int])
    assert_type(
        int_dict.discard_if(
            "key", condition=ETAG_HAS_CHANGED,
            expected_etag=ITEM_NOT_AVAILABLE),
        ConditionalOperationResult[int])
    assert_type(
        int_dict.transform_item("key", transformer=lambda v: 0),
        OperationResult[int])

    # TransformingFunction is a generic Protocol
    def int_transformer(
            current: int | ItemNotAvailableFlag,
    ) -> int | KeepCurrentFlag | DeleteCurrentFlag:
        if current is ITEM_NOT_AVAILABLE:
            return 0
        return current + 1

    assert_type(
        int_dict.transform_item("key", transformer=int_transformer),
        OperationResult[int])

    def str_transformer(
            current: str | ItemNotAvailableFlag,
    ) -> str | KeepCurrentFlag | DeleteCurrentFlag:
        if current is ITEM_NOT_AVAILABLE:
            return "default"
        return current + "_updated"

    assert_type(
        str_dict.transform_item("key", transformer=str_transformer),
        OperationResult[str])

    # Unparameterized usage (backward compatible)
    any_dict = cast(FileDirDict, None)
    assert_type(any_dict["key"], Any)
    assert_type(any_dict.get("key"), Any)
    assert_type(any_dict.pop("key", None), Any)

    # EmptyDict with type parameter
    empty = cast(EmptyDict[float], None)
    assert_type(empty.get("key"), float | None)
    assert_type(empty.get("key", 1.0), float)
    assert_type(empty.setdefault("key", 1.0), float)
    assert_type(empty.pop("key", 1.0), float)
    assert_type(empty.timestamp("key"), float)


def test_runtime_imports() -> None:
    """Ensure referenced typing targets exist at runtime."""

    from persidict import (
        ConditionalOperationResult, EmptyDict, FileDirDict,
        LocalDict, NonEmptyPersiDictKey, OperationResult,
        PersiDictKey, TransformingFunction,
    )

    assert EmptyDict is not None
    assert FileDirDict is not None
    assert LocalDict is not None
    assert PersiDictKey is not None
    assert NonEmptyPersiDictKey is not None
    assert OperationResult is not None
    assert ConditionalOperationResult is not None
    assert TransformingFunction is not None


def test_transforming_function_protocol_isinstance() -> None:
    """TransformingFunction is runtime-checkable: callables satisfy it."""
    from persidict import TransformingFunction

    def named_fn(v):
        return v

    assert isinstance(named_fn, TransformingFunction)
    assert isinstance(lambda v: v, TransformingFunction)
    assert not isinstance("not_callable", TransformingFunction)
    assert not isinstance(42, TransformingFunction)


def test_transforming_function_generic_subscript() -> None:
    """TransformingFunction[T] can be subscripted at runtime."""
    from persidict import TransformingFunction

    parametrized = TransformingFunction[int]
    assert parametrized is not None
    # Subscripting twice with different types yields distinct objects
    assert TransformingFunction[int] is not TransformingFunction[str]
