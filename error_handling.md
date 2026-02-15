# persidict error handling guidelines

This document defines **user-facing exception semantics** and **maintainer rules** for raising/translation so behavior is predictable across backends and wrappers.

## Two reporting channels

Persidict reports problems through two fundamentally different channels. Every method uses exactly one.

**Exceptions** — used by the dict-protocol API (`__getitem__`, `__setitem__`, `__delitem__`, `pop`, etc.). Python callers expect these methods to raise on failure, so we follow that contract.

**Result objects** — used by the conditional `_if` API (`get_item_if`, `set_item_if`, `setdefault_if`, `discard_item_if`) and `transform_item`. These methods are designed for compare-and-swap loops where exceptions would be expensive control flow. They return `ConditionalOperationResult` / `OperationResult` with sentinel fields (`ITEM_NOT_AVAILABLE`, `VALUE_NOT_RETRIEVED`) instead of raising.

A third category — **silent success** — applies to methods whose callers explicitly opted out of failure notification: `discard()` returns `bool`, joker assignments (`d[k] = KEEP_CURRENT`) are no-ops when there's nothing to do.

**Decision rule for new methods:** use exceptions if the method participates in the standard dict protocol or has a single expected outcome; use result objects if the method is designed for optimistic concurrency loops; use silent success only for fire-and-forget convenience wrappers around exception-raising primitives.

---

## Exception taxonomy

### Standard exceptions

- **`KeyError(key)`**: key/object is not present.
- **`TypeError`**: wrong argument type, wrong sentinel/protocol object, wrong signature/arity.
- **`ValueError`**: argument value invalid (range, enum-like flags, unsafe strings, invalid config values).
- **`NotImplementedError`**: abstract base method that a subclass is **expected to override**. Not for operations a concrete class intentionally refuses — that's `MutationPolicyError` (policy-based refusal) or `TypeError` (structurally incompatible, like `__getitem__` on `OverlappingMultiDict`).

### persidict custom exceptions

#### `MutationPolicyError(TypeError)`
Raised when the **dict’s mutation policy forbids the attempted mutation**.

Examples:
- append-only: overwrite/delete/clear/discard
- write-once: attempt to set an existing key to a different value
- wrappers that expose read-only views

#### `ConcurrencyConflictError(RuntimeError)`
Raised when an operation **fails after exhausting retries** due to **concurrent modification / CAS conflicts / ETag mismatches**.

Examples:
- `transform_item` retry exhaustion
- `setdefault`/CAS loops that retry and still lose the race

#### `BackendError(RuntimeError)`
Raised when a backend/infrastructure condition prevents completion and is **not** a missing-key condition.

Examples:
- cannot create required directories
- permission/auth failures
- network/IO/throttling errors
- bucket/prefix inaccessible
- corruption/partial-write conditions detected by the backend

**Must** preserve the original exception via chaining: `raise BackendError(...) from exc`

---

## Mapping from scenarios to exceptions

| Scenario | Result |
|---|---|
| Key not found (any backend) | `KeyError(key)` |
| `get(key, default)` with missing key | return `default` (no exception) |
| `pop(key, default)` with missing key | return `default` (no exception) |
| `popitem()` on empty | `KeyError` |
| Invalid argument type (incl. wrong sentinel/protocol object) | `TypeError` |
| Invalid argument value (ranges/flags/config) | `ValueError` |
| Abstract method not yet overridden by subclass | `NotImplementedError` |
| Operation structurally incompatible with class (e.g. `__getitem__` on `OverlappingMultiDict`) | `TypeError` |
| Mutation refused by policy (append-only/write-once/read-only) | `MutationPolicyError` |
| Conditional op “condition not met” | return status (`False` / “not satisfied”) |
| Retry exhaustion due to concurrency | `ConcurrencyConflictError` |
| Backend/infrastructure failure (not missing key) | `BackendError` (chained) |

---

## Rules for translating backend exceptions

1. **Translate “not found” to `KeyError(key)`**:
   - Always use exception chaining: `raise KeyError(key) from exc`.

2. **Wrap all other backend/infrastructure failures in `BackendError`**:
   - `raise BackendError(backend=..., operation=..., key=..., resource=...) from exc`
   - Do **not** emit ad-hoc `RuntimeError("...")` for backend failures.

3. **Do not catch or wrap control-flow/system exceptions**:
   - Never convert `KeyboardInterrupt`, `SystemExit`, `GeneratorExit`.

4. **Only catch backend exceptions you can classify**:
   - Translate **expected** error codes: 404 → `KeyError`, 412 → condition-not-met result, known infrastructure failures → `BackendError`.
   - For anything you **don't recognize**, use bare `raise`. Never write broad `except ClientError` or `except OSError` blocks that swallow or mis-translate unknown errors.
   - The `else: raise` branch after every error-code check is mandatory, not optional.

5. **Swallow exceptions only when all three conditions hold**:
   - The failure is **benign and expected** (race condition, idempotent cleanup, feature-detection probe).
   - There is **no meaningful action** the caller could take.
   - A **comment in the code** explains both what is caught and why it is safe to ignore.
   - Examples: bucket-already-exists on create, 404 on delete of already-deleted object, file vanishing between listing and reading during iteration, directory fsync failure.

---

## Mutation policy rules

### Single rule
If the dict refuses to perform a mutation **because of policy**, raise **`MutationPolicyError`**.

### Concrete policy guidance
- **append-only**
  - allow: setting a *new* key
  - forbid: overwrite, delete, clear, discard, delete-via-write (`DELETE_CURRENT`) → `MutationPolicyError(policy="append_only", ...)`
- **write-once**
  - allow: first write
  - forbid: setting an existing key to a different value → `MutationPolicyError(policy="write_once", ...)`

---

## Concurrency and retries

- Any operation that performs a CAS/ETag retry loop must:
  - validate retry params (`TypeError`/`ValueError`)
  - retry only on clearly-defined conflict signals
  - on exhaustion: raise **`ConcurrencyConflictError`** with `operation`, `attempts`, and `key` if available
  - preserve the last conflict exception as `__cause__` when useful

---

## Scenarios that do not raise exceptions

### Dict-protocol methods that absorb missing keys
- `get(key, default)` / `pop(key, default)` — return default.
- `discard(key)` — returns `False`.
- `key in d` — returns `False` (translates backend 404 to `bool`).
- `__eq__` — swallows `KeyError`/`TypeError`/`AttributeError`/`ValueError`, returns `False`.
- Iteration (`keys`, `values`, `items`) — silently skips entries that vanish between listing and reading (race condition).
- `popitem` — silently retries the next key when one is concurrently deleted; raises `KeyError` only when the dict is entirely empty.

### Joker assignments
- `d[k] = KEEP_CURRENT` — no-op if key exists or is absent.
- `d[k] = DELETE_CURRENT` — calls `discard(k)`, no error if absent.

### Conditional `_if` methods
- Unmet ETag preconditions are reported via `condition_was_satisfied=False` in the result object, never via exception.
- Missing keys produce `actual_etag=ITEM_NOT_AVAILABLE` and `new_value=ITEM_NOT_AVAILABLE`.
- Skipped value retrieval produces `new_value=VALUE_NOT_RETRIEVED`.

### Transient failures recovered by retries before surfacing `ConcurrencyConflictError`
- Filesystem `PermissionError`.
- ETag mismatch during read (stat-read-stat).
- CAS conflicts in `transform_item` / `setdefault`.

### Swallowed backend errors (benign, documented in code)
- Bucket-already-exists on create, 403 on `head_bucket` (cross-account bucket).
- 404 on `delete_object` of an already-deleted key.
- File/directory removal failures during `clear()` (partial cleanup is acceptable).
- Directory fsync failure (metadata durability is best-effort).
- Feature-detection probe failures (fall back to non-conditional code path).

---

## Implementation practices

### 1) Use centralized helper functions
Use helpers to enforce consistent types, payloads, and chaining.

- `raise_missing_key(key, *, cause=None)`
- `raise_policy_violation(policy, operation, key=None, *, details=None, cause=None)`
- `raise_concurrency_conflict(key=None, operation=None, attempts=None, *, max_retries=None, cause=None)`
- `raise_backend_failure(backend, operation, key=None, resource=None, *, cause=None)`

### 2) Exception chaining and rethrowing
- **Translating** (e.g. `FileNotFoundError` → `KeyError`): always chain with `raise KeyError(key) from exc` so the original traceback is preserved for debugging.
- **Rethrowing unchanged**: use bare `raise` (never `raise e`, which resets the traceback).
- **Suppressing the chain** (`raise X from None`): use only when the original exception is purely an implementation detail that would confuse callers (rare).

### 3) Message and payload conventions
- `KeyError` argument must be the **raw key** (`KeyError(key)`), matching Python `dict` convention. Never embed backend details ("File ... does not exist", "S3 bucket ...") — this leaks the abstraction.
- `MutationPolicyError` messages should name the **policy** that was violated, not the operation that failed. Good: `"append-only dict does not allow deletion"`. Bad: `"Can't modify an immutable key-value pair"` (doesn't say which policy or what was attempted).
- Custom exceptions carry **structured fields** (`key`, `attempts`, `policy`, etc.) for programmatic access; the human-readable `str` is secondary.
- Never leak secrets (absolute paths, credentials). Put sensitive backend detail in `__cause__` when needed.

---