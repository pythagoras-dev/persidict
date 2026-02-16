# persidict error handling guidelines

This document defines the conceptual error-handling model for persidict: how problems are reported to callers, what exception types exist, and how backends translate their native errors into that shared vocabulary.

## Reporting channels

Persidict reports problems through two channels. Every method uses exactly one.

**Exceptions** — used by the dict-protocol API (`__getitem__`, `__setitem__`, `__delitem__`, `pop`, etc.). Python callers expect these methods to raise on failure, so we follow that contract.

**Result objects** — used by the conditional `_if` API and `transform_item`. These methods are designed for compare-and-swap loops where exceptions would be expensive control flow. They return `ConditionalOperationResult` / `OperationResult` with sentinel fields instead of raising for key-absence and condition-not-met outcomes. Result-object methods still raise `TypeError`, `ValueError`, `MutationPolicyError`, and `BackendError` — only the expected outcome variability (key present/absent, condition met/unmet) is reported via the result object.

Some methods absorb conditions that would otherwise be errors: `discard()` returns `bool`, `get()` returns a default, joker assignments are no-ops when there's nothing to do. These are convenience wrappers, not a separate channel.

**Decision rule for new methods:** use exceptions for dict-protocol and single-outcome methods; use result objects for optimistic concurrency loops; use the absorbing-convenience pattern only for fire-and-forget wrappers around exception-raising primitives.

---

## Exception taxonomy

### Standard exceptions

- **`KeyError(key)`**: key not present. Argument is always the raw key, never a message string with backend details.
- **`TypeError`**: wrong argument type, wrong sentinel/protocol object, structurally incompatible operation.
- **`ValueError`**: argument value invalid (range, flags, config).
- **`NotImplementedError`**: abstract method a subclass is expected to override. Not for intentional refusals — those are `MutationPolicyError` or `TypeError`.

### persidict custom exceptions

- **`MutationPolicyError(TypeError)`** — the dict's mutation policy (append-only, write-once, read-only) forbids the attempted mutation. Messages name the policy, not the operation.

- **`ConcurrencyConflictError(RuntimeError)`** — an operation failed after exhausting retries due to concurrent modification. Carries structured context (key, attempts) for programmatic access.

- **`BackendError(RuntimeError)`** — a backend/infrastructure condition (permissions, network, corruption) prevents completion and is not a missing-key condition. Must preserve the original exception via chaining (`from exc`). Carries enough context (backend, operation, key) for debugging.

---

## Translating backend exceptions

- **Translate "not found" to `KeyError(key)`**, always with exception chaining. Backends must translate all not-found signals (including `FileNotFoundError`, S3 404) into `KeyError(key)` at the backend boundary. Code above the backend layer catches only `KeyError`, never `FileNotFoundError`.
- **Wrap other infrastructure failures in `BackendError`**, always with exception chaining. No ad-hoc `RuntimeError`.
- **Only catch exceptions you can classify.** Translate known error codes (404 → `KeyError`, 412 → condition-not-met, known infra failures → `BackendError`). Re-raise anything unrecognized with bare `raise`.
- **Never catch or wrap** `KeyboardInterrupt`, `SystemExit`, `GeneratorExit`.
- **Swallow exceptions only when** the failure is benign and expected, the caller can't act on it, and a code comment explains why it's safe.

---

## Mutation policy

If a mutation is refused because of policy, raise `MutationPolicyError` naming the policy. Per-policy allow/forbid details live in the code and class docstrings, not here.

---

## Concurrency and retries

CAS/ETag retry loops retry only on clearly-defined conflict signals. On exhaustion, raise `ConcurrencyConflictError`. Preserve the last conflict exception as `__cause__` when useful.

---

## When exceptions are not raised

Three principles govern exception-free outcomes:

1. **Dict-protocol convenience methods absorb missing keys** — `get()`, `pop()` with defaults, `discard()`, `__contains__`, `__eq__`, and iteration all convert absence or transient race conditions into return values rather than exceptions.

2. **Conditional and joker APIs report outcomes via return values** — unmet preconditions produce result-object fields (`condition_was_satisfied=False`, sentinel values), never exceptions. Joker assignments (`KEEP_CURRENT`, `DELETE_CURRENT`) are no-ops when there's nothing to do.

3. **Backends may swallow benign, idempotent failures** — e.g. already-exists on create, 404 on delete of an already-deleted object, fsync failures. Each must be documented with a code comment.

---

## Implementation practices

- **Centralized helpers** — use helper functions to enforce consistent exception types, payloads, and chaining across backends.
- **Exception chaining** — always chain when translating (`raise X from exc`). Use bare `raise` when rethrowing unchanged (never `raise exc`).
- **Structured fields over messages** — custom exceptions carry structured fields (`key`, `attempts`, `policy`, etc.) for programmatic access; the human-readable `str` is secondary. Never leak secrets in messages.
- **Wrappers pass through** — wrappers let inner-dict exceptions propagate unchanged. A wrapper only raises its own `MutationPolicyError` for policies it enforces *before* calling the inner dict.

---
