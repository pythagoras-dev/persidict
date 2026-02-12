# Persidict ETag & Conditional Operations — Requirements

## Problem

Multiple processes (or machines) may read and write the same persistent dictionary concurrently. Without coordination, a classic race condition occurs:

1. Process A reads item, gets value V1
2. Process B reads same item, gets value V1
3. Process A writes V2
4. Process B writes V3, silently overwriting A's change

This is the **lost update** problem.

## Solution: Optimistic Concurrency via ETags

Every stored item carries an **ETag** — an opaque version string that changes whenever the stored representation changes; backends may or may not change the ETag on idempotent writes. Callers read the ETag, do their work, then submit a conditional write: "store this new value, but **only if the ETag is still what I saw**." If someone else wrote in between, the ETag won't match and the operation reports failure instead of silently clobbering.

This is the same model used by HTTP (`If-Match` / `If-None-Match`) and S3 conditional requests.

## Atomicity Matrix

The following table summarizes which operation × backend combinations are atomic (free of TOCTOU races). This is the single most important property of the system — consult it before choosing a backend.

| Operation | `BasicS3Dict` | `FileDirDict` | `LocalDict` | `MutableDictCached` | `AppendOnlyDictCached` |
|---|---|---|---|---|---|
| `get_item_if` | **Atomic** (S3 conditional GET) | Best-effort | Best-effort | Delegates to `main_dict` | Delegates to `main_dict` |
| `set_item_if` | **Atomic** (S3 conditional PUT) | Best-effort | Best-effort | Delegates to `main_dict` | Delegates to `main_dict` |
| `setdefault_if` | **Atomic** (`IfNoneMatch: *`) | Best-effort | Best-effort | Delegates to `main_dict` | Delegates to `main_dict` |
| `discard_item_if` | **Atomic** (S3 conditional DELETE) | Best-effort | Best-effort | Delegates to `main_dict` | N/A (`append_only`) |
| `transform_item` | **Atomic** (via atomic conditional ops) | Best-effort | Best-effort | Delegates to `main_dict` | N/A (`append_only`) |
| `etag` | S3-native ETag | `mtime_ns:file_size` (weak) | Monotonic counter (strong) | Cached, from `main_dict` | From `main_dict` |

**Key:**
- **Atomic** — the ETag check and the mutation happen as a single server-side operation; no concurrent writer can slip in between.
- **Best-effort** — the ETag check and the mutation are separate steps (check-then-act); safe for single-process use, but concurrent writers can interleave between them. Suitable when external coordination is provided or races are tolerable.
- **Delegates to `main_dict`** — the caching layer forwards the call to the underlying backend and updates its caches as a side effect. Atomicity depends entirely on the `main_dict` backend.
- **N/A** — operation is not supported (raises `TypeError` or `NotImplementedError`).

### Testable invariants

- **No lost updates (atomic backends):** Under concurrent writers, `set_item_if` with `ETAG_IS_THE_SAME` on `BasicS3Dict` must never produce a lost update. If two processes race, exactly one succeeds and the other receives `condition_was_satisfied=False`.
- **Insert-if-absent (atomic backends):** `setdefault_if` with `ETAG_IS_THE_SAME` + `expected=ITEM_NOT_AVAILABLE` on `BasicS3Dict` must ensure at most one writer inserts the key. All others receive the existing value.
- **Delete-known-version (atomic backends):** `discard_item_if` with `ETAG_IS_THE_SAME` on `BasicS3Dict` must not delete a version that differs from the expected ETag.
- **Cache coherence:** After a successful conditional write through `MutableDictCached`, the cache must reflect the written value and its resulting ETag. After a failed conditional write, the cache must not reflect the *proposed* value. However, when the operation retrieves the current value from the backend (i.e. `retrieve_value=ALWAYS_RETRIEVE`), caches may be updated to reflect the *actual* backend state (see R11).

## Requirements

### R1. Every item has a version identifier (ETag)

- `etag(key)` returns an opaque string intended to change when the item's value changes.
- If the key does not exist, `etag()` raises `KeyError`.
- ETag stability: calling `etag()` twice without an intervening write returns the same value.
- ETag is opaque; do not assume a new ETag after a write if the value is identical.

**Weak semantics note:** ETags are best-effort version identifiers, not a
cryptographic guarantee. `FileDirDict` uses timestamp-derived ETags, which can
repeat under coarse filesystem resolution, and even native S3 ETags have
a non-zero theoretical collision probability. `LocalDict` uses a monotonic
integer counter, which eliminates collisions within a single process. Treat
ETag matches as a strong hint, not absolute proof of identity.

### R2. Three condition modes

| Condition | Meaning |
|---|---|
| `ANY_ETAG` | Unconditional — always proceed |
| `ETAG_IS_THE_SAME` | Proceed only if the item's current ETag equals the expected ETag |
| `ETAG_HAS_CHANGED` | Proceed only if the item's current ETag differs from the expected ETag |

### R3. Absent keys participate uniformly

- `ITEM_NOT_AVAILABLE` is used in place of an ETag when a key does not exist.
- It participates in condition evaluation: `ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE` is true, so `ETAG_IS_THE_SAME` with `expected=ITEM_NOT_AVAILABLE` is satisfied when the key is absent.
- This allows conditional insert-if-absent: "write only if the key still doesn't exist."

### R4. Four conditional operations

| Operation | Semantics |
|---|---|
| **get_item_if** | Read value, report whether condition held. Never mutates. |
| **set_item_if** | Write value only if condition is satisfied. Supports joker values (`KEEP_CURRENT`, `DELETE_CURRENT`). |
| **setdefault_if** | Insert default only if key is absent **and** condition is satisfied. If key exists, no mutation regardless of condition. |
| **discard_item_if** | Delete only if condition is satisfied. |

`setdefault_if` rejects joker values (`KEEP_CURRENT`, `DELETE_CURRENT`) with `TypeError`.

### R5. One unconditional read-modify-write operation

- **transform_item** reads the current value (or `ITEM_NOT_AVAILABLE`), passes it to a user-supplied transformer function, and writes the result back.
- The transformer may return `KEEP_CURRENT` (no-op) or `DELETE_CURRENT` (delete the key).
- `transform_item` supports bounded retries (`n_retries`) on ETag conflicts and may call the transformer multiple times under contention.

### R6. Structured results carry full state

Every conditional operation returns a result containing:
- Whether the condition was satisfied
- The actual ETag at check time
- The resulting ETag after the operation
- The value after the operation (or sentinel if absent / not retrieved)

This lets callers decide what to do next without an extra round-trip.

### R7. Bandwidth optimization

- `retrieve_value=IF_ETAG_CHANGED` allows skipping value retrieval when the caller already has a cached copy and only wants to know if the ETag changed. `retrieve_value=NEVER_RETRIEVE` unconditionally skips value retrieval.
- `VALUE_NOT_RETRIEVED` sentinel signals that the value exists but was not fetched.

### R8. Joker values as commands

- `KEEP_CURRENT` — passed as a value to mean "don't change anything" (useful in conditional pipelines where the decision to write is computed dynamically).
- `DELETE_CURRENT` — passed as a value to mean "delete the key" (unifies write and delete into a single API call).

## Atomicity Constraints

### R9. S3 backend must be atomic

`BasicS3Dict` must use S3 conditional request headers (`IfMatch`, `IfNoneMatch`) so that the ETag check and the mutation happen as a single server-side operation. No TOCTOU races.

### R10. File backend intentionally non-atomic

`FileDirDict` uses check-then-act (read ETag, then write). This is a deliberate choice: OS-native file locking (advisory or mandatory) is **not reliably propagated** across machines by sync services like Dropbox. Since `FileDirDict` must work with shared Dropbox folders, locking is not used. Callers who need atomicity on the filesystem should use a different coordination mechanism.

### R11. Caching layers preserve atomicity of the underlying backend

`MutableDictCached` and `AppendOnlyDictCached` delegate all conditional operations to their `main_dict`. Cache updates are side effects: successful writes update caches, and read paths refresh caches whenever a value is retrieved from the main dict, even if the condition failed. They never introduce their own TOCTOU window.

### R12. transform_item uses conditional operations

`transform_item` is implemented as a conditional-ops retry loop. It is atomic only when the backend's conditional operations are atomic; otherwise it remains non-atomic. On persistent conflicts it raises `TransformConflictError` after exhausting `n_retries` retries. For strict control, callers can still use explicit `get_item_if` + `set_item_if` loops or external synchronization.

## Failure Modes & Recovery

Conditional operations are designed to fail gracefully — they return structured results instead of raising exceptions for condition mismatches. However, callers must understand the distinct failure scenarios to handle them correctly.

### F1. ETag mismatch (condition not satisfied)

- **Trigger:** The item's current ETag does not satisfy the requested condition. Another process wrote or deleted the item between the caller's read and conditional write.
- **Applies to:** `set_item_if`, `setdefault_if`, `discard_item_if`, `get_item_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag` reflects the current state. `resulting_etag == actual_etag` (no mutation). `new_value` is the existing value (if `retrieve_value=ALWAYS_RETRIEVE`), `VALUE_NOT_RETRIEVED` (if `NEVER_RETRIEVE` or `IF_ETAG_CHANGED`), or `ITEM_NOT_AVAILABLE` (if the key is absent).
- **Recovery:** Re-read with `get_item_if` using `ANY_ETAG` to get the fresh value and ETag, then retry the conditional write. This is the standard optimistic-concurrency retry loop.

### F2. Key disappeared between read and write

- **Trigger:** The caller read an item and its ETag, but by the time the conditional write executes, the key no longer exists (another process deleted it).
- **Applies to:** `set_item_if`, `discard_item_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag=ITEM_NOT_AVAILABLE`. `resulting_etag=ITEM_NOT_AVAILABLE`. `new_value=ITEM_NOT_AVAILABLE`.
- **Recovery:** The caller can choose to re-insert (via `set_item_if` with `expected=ITEM_NOT_AVAILABLE`) or treat the deletion as authoritative and stop.

### F3. Key appeared between read and write (insert-if-absent race)

- **Trigger:** The caller intended to insert a new key (`expected=ITEM_NOT_AVAILABLE`, `condition=ETAG_IS_THE_SAME`), but another process inserted it first.
- **Applies to:** `set_item_if`, `setdefault_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag` is the ETag of the newly inserted item. `new_value` is the existing value (if retrieved). For `setdefault_if`, the existing value is returned without mutation regardless of condition outcome.
- **Recovery:** Accept the existing value, or re-read and retry with the new ETag if the caller needs to overwrite.

### F4. `TransformConflictError` (retries exhausted)

- **Trigger:** `transform_item` encountered ETag conflicts on every attempt and exhausted `n_retries`.
- **Applies to:** `transform_item` only.
- **Exception:** `TransformConflictError(key, attempts)` is raised (not a result — this is the only failure mode that raises).
- **Recovery:** The caller can retry with a higher `n_retries`, use `n_retries=None` for unbounded retries, or fall back to an explicit `get_item_if` + `set_item_if` loop with custom backoff. Persistent conflicts suggest high contention — consider redesigning the key space or using external coordination.

### F5. S3 conditional request failure (412/409)

- **Trigger:** S3 returns HTTP 412 Precondition Failed or 409 Conflict because the conditional header (`IfMatch`, `IfNoneMatch`) did not match the object's current ETag.
- **Applies to:** `BasicS3Dict` internal handling of `set_item_if`, `setdefault_if`, `discard_item_if`.
- **Visible effect:** The S3 error is caught internally and translated into a `ConditionalOperationResult` with `condition_was_satisfied=False`. The caller never sees the `ClientError` — it is the same as F1.
- **Recovery:** Same as F1.

### F6. Weak ETag collision (false match)

- **Trigger:** Two different values produce the same ETag. This can happen with timestamp-derived ETags on `FileDirDict` (coarse mtime resolution), and has a non-zero theoretical probability even for S3-native ETags. `LocalDict` uses a monotonic counter and is not susceptible to this failure mode within a single process.
- **Applies to:** All backends, but practically significant only for `FileDirDict`.
- **Visible effect:** `ETAG_IS_THE_SAME` incorrectly reports that the item has not changed. A conditional write may succeed when it should have failed, or `ETAG_HAS_CHANGED` may fail when the item actually did change.
- **Recovery:** No programmatic recovery. This is a known limitation documented in R1. Callers who require strong consistency must use `BasicS3Dict` or add external coordination.

### Failure identification quick reference

| Scenario | `condition_was_satisfied` | `actual_etag` | `resulting_etag` | `new_value` | Raises? |
|---|---|---|---|---|---|
| F1: ETag mismatch (key exists) | `False` | current ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F2: Key disappeared | `False` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` | No |
| F3: Key appeared (insert race) | `False` | new ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F4: Retries exhausted | N/A | N/A | N/A | N/A | `TransformConflictError` |
| F5: S3 412/409 | `False` | re-read ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F6: Weak ETag collision | `True` (incorrect) | stale ETag | may change | may be wrong value | No |

## Design Decisions

- **Singletons for sentinels** — `ITEM_NOT_AVAILABLE`, `KEEP_CURRENT`, etc. are singleton instances, enabling fast `is` identity checks.
- **Frozen dataclass results** — `ConditionalOperationResult` and `OperationResult` are immutable, preventing accidental mutation of returned state.
- **Uniform absent-key representation** — a single sentinel (`ITEM_NOT_AVAILABLE`) is used in all positions (expected_etag, actual_etag, resulting_etag, new_value, transformer input), avoiding null/None ambiguity.
