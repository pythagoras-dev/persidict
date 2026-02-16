"""Custom exception types for the persidict error-handling taxonomy.

Defines three exception classes:

- ``MutationPolicyError`` — mutation forbidden by dict policy.
- ``ConcurrencyConflictError`` — retries exhausted due to concurrent
  modification.
- ``BackendError`` — backend infrastructure failure (not missing-key).
"""

from __future__ import annotations

from typing import Any


class MutationPolicyError(TypeError):
    """The dict's mutation policy forbids the attempted mutation.

    Messages name the policy (e.g. ``"append-only"``, ``"write-once"``),
    not the operation.

    Args:
        policy: Name of the policy that rejected the mutation.

    Attributes:
        policy: Name of the policy that rejected the mutation.
    """

    def __init__(self, policy: str) -> None:
        super().__init__(policy)
        self.policy = policy


class ConcurrencyConflictError(RuntimeError):
    """An operation failed after exhausting retries due to concurrent modification.

    Carries structured context for programmatic access.

    Args:
        key: The key on which the conflict occurred.
        attempts: Total number of attempts made before giving up.

    Attributes:
        key: The key on which the conflict occurred.
        attempts: Total number of attempts made before giving up.
    """

    def __init__(self, key: Any, attempts: int) -> None:
        super().__init__(
            f"operation failed after {attempts} attempt(s) for key {key!r}")
        self.key = key
        self.attempts = attempts


class BackendError(RuntimeError):
    """A backend/infrastructure condition prevents completion.

    Not a missing-key condition — those are ``KeyError``. Must be raised
    with exception chaining (``raise BackendError(...) from exc``).

    Args:
        message: Human-readable description of the failure.
        backend: Name of the backend (e.g. ``"filesystem"``, ``"s3"``).
        operation: Name of the operation that failed (e.g. ``"init"``,
            ``"put_object"``).
        key: The key involved, or ``None`` if not applicable.

    Attributes:
        backend: Name of the backend.
        operation: Name of the failed operation.
        key: The key involved, or ``None``.
    """

    def __init__(
            self,
            message: str,
            *,
            backend: str,
            operation: str,
            key: Any = None,
    ) -> None:
        super().__init__(message)
        self.backend = backend
        self.operation = operation
        self.key = key
