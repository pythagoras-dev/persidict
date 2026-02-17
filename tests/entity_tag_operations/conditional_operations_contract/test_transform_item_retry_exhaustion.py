"""Tests that transform_item raises ConcurrencyConflictError after exhausting retries.

Verifies that when every conditional write reports a conflict, the retry
loop terminates after the configured number of retries and raises
ConcurrencyConflictError. Also checks that the transformer call count is
bounded to (1 + n_retries).
"""

import pytest

import persidict.persi_dict as persi_dict
from persidict import LocalDict, ConcurrencyConflictError
from persidict.jokers_and_status_flags import (
    IF_ETAG_CHANGED,
    VALUE_NOT_RETRIEVED,
    ConditionalOperationResult,
)


def test_transform_raises_after_n_retries_exhausted(monkeypatch):
    """ConcurrencyConflictError is raised after n_retries conflicts."""
    d = LocalDict(serialization_format="pkl")
    d["key"] = "value"

    def always_conflict_set_item_if(key, value, condition, expected_etag, *,
                                    retrieve_value=IF_ETAG_CHANGED):
        return ConditionalOperationResult(
            condition_was_satisfied=False,
            actual_etag=expected_etag,
            resulting_etag=expected_etag,
            new_value=VALUE_NOT_RETRIEVED,
        )

    monkeypatch.setattr(d, "set_item_if", always_conflict_set_item_if)
    monkeypatch.setattr(persi_dict.time, "sleep", lambda _: None)
    calls = []

    def transformer(value):
        calls.append(value)
        return "new"

    with pytest.raises(ConcurrencyConflictError) as exc_info:
        d.transform_item("key", transformer=transformer, n_retries=2)

    assert exc_info.value.attempts == 3
    assert len(calls) == 3


def test_transform_zero_retries_raises_after_one_attempt(monkeypatch):
    """With n_retries=0, only one attempt is made before raising."""
    d = LocalDict(serialization_format="pkl")
    d["key"] = "value"

    def always_conflict_set_item_if(key, value, condition, expected_etag, *,
                                    retrieve_value=IF_ETAG_CHANGED):
        return ConditionalOperationResult(
            condition_was_satisfied=False,
            actual_etag=expected_etag,
            resulting_etag=expected_etag,
            new_value=VALUE_NOT_RETRIEVED,
        )

    monkeypatch.setattr(d, "set_item_if", always_conflict_set_item_if)
    monkeypatch.setattr(persi_dict.time, "sleep", lambda _: None)
    calls = []

    def transformer(value):
        calls.append(value)
        return "new"

    with pytest.raises(ConcurrencyConflictError) as exc_info:
        d.transform_item("key", transformer=transformer, n_retries=0)

    assert exc_info.value.attempts == 1
    assert len(calls) == 1


def test_transform_succeeds_when_conflict_resolves_before_exhaustion(monkeypatch):
    """No error when a conflict resolves within the retry budget."""
    d = LocalDict(serialization_format="pkl")
    d["key"] = "original"

    attempt = [0]
    original_set_item_if = d.set_item_if

    def conflict_then_succeed(key, value, condition, expected_etag, *,
                              retrieve_value=IF_ETAG_CHANGED):
        attempt[0] += 1
        if attempt[0] <= 2:
            return ConditionalOperationResult(
                condition_was_satisfied=False,
                actual_etag=expected_etag,
                resulting_etag=expected_etag,
                new_value=VALUE_NOT_RETRIEVED,
            )
        return original_set_item_if(
            key, value=value, condition=condition,
            expected_etag=expected_etag, retrieve_value=retrieve_value)

    monkeypatch.setattr(d, "set_item_if", conflict_then_succeed)
    monkeypatch.setattr(persi_dict.time, "sleep", lambda _: None)

    result = d.transform_item("key", transformer=lambda v: "updated", n_retries=5)

    assert result.new_value == "updated"
    assert d["key"] == "updated"
