# Persidict ETag & Conditional Operations

## The Concurrent Access Problem

Multiple processes (or machines) may read and write the same persistent dictionary concurrently. Without coordination, a classic race condition occurs:

1. Process A reads item, gets value V1
2. Process B reads same item, gets value V1
3. Process A writes V2
4. Process B writes V3, silently overwriting A's change

This is the **lost update** problem.

A secondary concern is **wasted IO**. When a caller already has a cached copy of a value, re-reading it from the backend just to confirm it hasn't changed transfers data unnecessarily — a significant cost when values are large or the backend is remote (e.g. S3).

## Persidict's Solution: Optimistic Concurrency via ETags

Persidict addresses both problems using **ETags** — opaque version strings that change whenever a stored item changes. Every stored item carries an ETag, and backends may or may not change it on idempotent writes.

ETags enable two critical capabilities:

1. **Correctness (preventing lost updates).** Callers read the ETag along with the value, do their work, then submit a conditional write: "store this new value, but **only if the ETag is still what I saw**." If someone else wrote in between, the ETag won't match and the operation reports failure instead of silently clobbering. This is optimistic concurrency — processes proceed without locks and detect conflicts at write time.

2. **IO efficiency (skipping redundant reads).** Callers can ask "give me the value, but **only if the ETag has changed** since I last looked." If it hasn't changed, the backend skips transferring the value — the same idea as HTTP `304 Not Modified`. This is controlled by the `retrieve_value` parameter on every read-capable operation, with three modes: `ALWAYS_RETRIEVE`, `IF_ETAG_CHANGED` (the default), and `NEVER_RETRIEVE`.

This model follows the same principles as HTTP conditional requests (`If-Match` / `If-None-Match`) and S3 conditional operations.

---

## Conditional Operations API

Persidict provides four conditional operations that take an `expected_etag` and a `condition` parameter. Each returns a structured result indicating whether the condition was satisfied, what the actual ETag was, and what the resulting state is. This lets callers implement optimistic concurrency loops without extra round-trips.

### `get_item_if(key, *, condition, expected_etag, retrieve_value=IF_ETAG_CHANGED)`

Read-only operation that never mutates state. The result's `condition_was_satisfied` field tells you whether the condition held. The default `retrieve_value=IF_ETAG_CHANGED` skips the value transfer when the ETag matches, making this the primary tool for IO-efficient cache validation.

```python
r = d.get_item_if(k, condition=ETAG_HAS_CHANGED, expected_etag=cached_etag, retrieve_value=IF_ETAG_CHANGED)
if r.condition_was_satisfied:
    # ETag changed -> r.new_value has fresh data
else:
    # r.new_value is VALUE_NOT_RETRIEVED -> use your cached copy
```

### `set_item_if(key, *, value, condition, expected_etag, retrieve_value=IF_ETAG_CHANGED)`

Conditional write operation — the workhorse of optimistic concurrency. The `value` parameter accepts a real value, `KEEP_CURRENT` (no-op), or `DELETE_CURRENT` (delete). On condition failure, no mutation occurs. The `retrieve_value` parameter controls whether the existing value is fetched on failure — use `NEVER_RETRIEVE` when only the ETag matters for your retry logic.

```python
r = d.get_item_if(k, condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)
new_val = transform(r.new_value)
r2 = d.set_item_if(k, value=new_val, condition=ETAG_IS_THE_SAME, expected_etag=r.actual_etag)
if not r2.condition_was_satisfied:
    pass  # conflict — retry
```

### `setdefault_if(key, *, default_value, condition, expected_etag, retrieve_value=IF_ETAG_CHANGED)`

Conditional insert-if-absent operation. If the key already exists, returns the existing value without mutation regardless of condition. This differs from `set_item_if`, which would overwrite when the condition is satisfied. The `default_value` must be a real value (not `KEEP_CURRENT` or `DELETE_CURRENT`).

```python
r = d.setdefault_if(k, default_value=initial_value, condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)
```

### `discard_if(key, *, condition, expected_etag)`

Conditional delete operation. Unlike the read-capable operations, this has no `retrieve_value` parameter. On condition failure, `new_value` is `VALUE_NOT_RETRIEVED` (or `ITEM_NOT_AVAILABLE` if the key is missing); on success, `new_value` is `ITEM_NOT_AVAILABLE`.

```python
r = d.discard_if(k, condition=ETAG_IS_THE_SAME, expected_etag=known_etag)
```

### `transform_item(key, *, transformer, n_retries=6)`

Higher-level read-modify-write operation implemented as a conditional-ops retry loop. The `transformer` function receives the current value (or `ITEM_NOT_AVAILABLE` if absent) and returns the new value, `KEEP_CURRENT`, or `DELETE_CURRENT`. Persidict automatically handles ETag conflicts and retries, making this the simplest way to implement atomic updates. The operation is atomic only when the backend's conditional operations are atomic; on persistent conflicts it raises `ConcurrencyConflictError`. The transformer may be called multiple times under contention.

```python
def increment(v):
    if v is ITEM_NOT_AVAILABLE:
        return 1
    return v + 1

r = d.transform_item(k, transformer=increment)
```

---

## Core Concepts

### ETags as Version Identifiers

Persidict's `etag(key)` method returns an opaque string that changes when the item's stored representation changes. If the key does not exist, `etag()` raises `KeyError`. Calling `etag()` twice without an intervening write returns the same value, ensuring stability for optimistic concurrency checks. ETags are opaque — backends may or may not generate a new ETag after a write if the value is identical.

**Important:** ETags are best-effort version identifiers, not cryptographic guarantees. `FileDirDict` uses stat-derived ETags (`mtime_ns:size:inode`), which can repeat under coarse filesystem resolution. Even native S3 ETags have a non-zero theoretical collision probability. `LocalDict` uses a monotonic integer counter, which eliminates collisions within a single process. Treat ETag matches as a strong hint, not absolute proof of identity.

### Condition Modes

Conditional operations accept one of three condition flags:

| Condition | Meaning |
|---|---|
| `ANY_ETAG` | Unconditional — always proceed |
| `ETAG_IS_THE_SAME` | Proceed only if the item's current ETag equals the expected ETag |
| `ETAG_HAS_CHANGED` | Proceed only if the item's current ETag differs from the expected ETag |

The condition logic is straightforward:

| Condition | `expected == actual` | `expected != actual` |
|---|---|---|
| `ANY_ETAG` | **satisfied** | **satisfied** |
| `ETAG_IS_THE_SAME` | **satisfied** | not satisfied |
| `ETAG_HAS_CHANGED` | not satisfied | **satisfied** |

### Absent Keys Participate in Conditions

When a key does not exist, persidict uses the sentinel `ITEM_NOT_AVAILABLE` in place of an ETag. This sentinel participates in condition evaluation: `ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE` is true, so `ETAG_IS_THE_SAME` with `expected=ITEM_NOT_AVAILABLE` is satisfied when the key is absent. This enables conditional insert-if-absent: "write only if the key still doesn't exist."

### Structured Results

Every conditional operation returns a result containing:
- Whether the condition was satisfied
- The actual ETag at check time
- The resulting ETag after the operation
- The value after the operation (or sentinel if absent / not retrieved)

This design lets callers decide what to do next without an extra round-trip.

### IO Optimization via Selective Retrieval

Transferring values is the dominant IO cost, especially for large values on remote backends like S3. Every read-capable operation accepts a `retrieve_value` parameter:

| Mode | Behavior |
|---|---|
| `ALWAYS_RETRIEVE` | Always fetch the value. |
| `IF_ETAG_CHANGED` (default) | Fetch the value only if the ETag differs from the expected one. When the ETag matches, the caller's cached copy is still valid and the backend skips the transfer. |
| `NEVER_RETRIEVE` | Never fetch the value — only return ETag information. |

When retrieval is skipped, the result carries `VALUE_NOT_RETRIEVED` in the `new_value` field, signaling that the value exists but was not fetched.

---

## Backend Atomicity Guarantees

The atomicity of conditional operations depends on the backend implementation. This is the most critical factor when choosing a backend for concurrent use cases.

### Atomicity matrix

| Operation | `BasicS3Dict` | `FileDirDict` | `LocalDict` | `MutableDictCached` | `AppendOnlyDictCached` |
|---|---|---|---|---|---|
| `get_item_if` | **Atomic** (S3 conditional GET) | Best-effort | Single-threaded | Delegates to `main_dict` | Delegates to `main_dict` |
| `set_item_if` | **Atomic** (S3 conditional PUT) | Best-effort | Single-threaded | Delegates to `main_dict` | Delegates to `main_dict` |
| `setdefault_if` | **Atomic** (`IfNoneMatch: *`) | Best-effort | Single-threaded | Delegates to `main_dict` | Delegates to `main_dict` |
| `discard_if` | **Atomic** (S3 conditional DELETE) | Best-effort | Single-threaded | Delegates to `main_dict` | N/A (`append_only`) |
| `transform_item` | **Atomic** (via atomic conditional ops) | Best-effort | Single-threaded | Delegates to `main_dict` | N/A (`append_only`) |
| `etag` | S3-native ETag | `mtime_ns:size:inode` (weak) | Monotonic counter (strong) | Cached, from `main_dict` | From `main_dict` |

**Key:**
- **Atomic** — the ETag check and the mutation happen as a single server-side operation; no concurrent writer can slip in between.
- **Best-effort** — the ETag check and the mutation are separate steps (check-then-act); safe for single-process use, but concurrent writers can interleave between them. Suitable when external coordination is provided or races are tolerable.
- **Single-threaded** — `LocalDict` is not thread-safe; conditional operations use check-then-act, but no concurrent writer can interleave in single-threaded use.
- **Delegates to `main_dict`** — the caching layer forwards the call to the underlying backend and updates its caches as a side effect. Atomicity depends entirely on the `main_dict` backend.
- **N/A** — operation is not supported (raises `TypeError` or `NotImplementedError`).

### ETag implementations by backend

| Backend | `etag()` source | Conditional ops atomicity |
|---|---|---|
| `BasicS3Dict` | S3 native ETag (`head_object`) | Atomic (S3 conditional headers) |
| `FileDirDict` | `mtime_ns:size:inode` (stat-based, weak) | Non-atomic (check-then-act)\* |
| `LocalDict` | Monotonic write counter (integer, strong) | Single-threaded (check-then-act) |

\* `FileDirDict` intentionally avoids OS-native file locking because it must work with shared folders synced via Dropbox (and similar services), where advisory/mandatory locks are not reliably propagated across machines.

### Key atomicity properties

- **No lost updates (atomic backends):** Under concurrent writers on `BasicS3Dict`, `set_item_if` with `ETAG_IS_THE_SAME` never produces a lost update. If two processes race, exactly one succeeds and the other receives `condition_was_satisfied=False`.
- **Insert-if-absent (atomic backends):** `setdefault_if` with `ETAG_IS_THE_SAME` + `expected=ITEM_NOT_AVAILABLE` on `BasicS3Dict` ensures at most one writer inserts the key. All others receive the existing value.
- **Delete-known-version (atomic backends):** `discard_if` with `ETAG_IS_THE_SAME` on `BasicS3Dict` never deletes a version that differs from the expected ETag.
- **Cache coherence:** Caching layers (`MutableDictCached`, `AppendOnlyDictCached`) delegate to their `main_dict` and update caches as side effects. They never introduce their own TOCTOU window.

### S3: True Atomicity

`BasicS3Dict` uses S3 conditional request headers (`IfMatch`, `IfNoneMatch`) so the ETag check and mutation happen as a single server-side operation — no TOCTOU races.

### FileDirDict: Intentionally Non-Atomic

`FileDirDict` uses check-then-act (read ETag, then write). This is deliberate: OS-native file locking is **not reliably propagated** across machines by sync services like Dropbox. Since `FileDirDict` must work with shared folders, locking is not used. Callers who need atomicity on the filesystem should use external coordination.

### LocalDict: Single-Threaded Only

`LocalDict` is not thread-safe. Conditional operations use check-then-act, but no concurrent writer can interleave in single-threaded use.

### transform_item Inherits Backend Atomicity

`transform_item` is implemented as a conditional-ops retry loop. It is atomic only when the backend's conditional operations are atomic. On persistent conflicts it raises `ConcurrencyConflictError` after exhausting `n_retries`. For strict control, callers can use explicit `get_item_if` + `set_item_if` loops or external synchronization.

---

## Types and Sentinels

Persidict uses typed sentinels to distinguish between different states without `None` ambiguity.

### ETag Values

| Type | Meaning |
|---|---|
| `ETagValue` (`str`) | Opaque version identifier for a stored item |
| `ITEM_NOT_AVAILABLE` | Key is absent (used in place of an ETag when key doesn't exist) |

### Condition Flags

| Flag | Semantics |
|---|---|
| `ANY_ETAG` | Always satisfied (unconditional) |
| `ETAG_IS_THE_SAME` | `expected == actual` |
| `ETAG_HAS_CHANGED` | `expected != actual` |

### Joker Values

These can be passed as the `value` parameter to `set_item_if` (but not `setdefault_if`):

| Flag | Effect when written |
|---|---|
| `KEEP_CURRENT` | No-op; keep existing value unchanged |
| `DELETE_CURRENT` | Delete the key (same as `discard`) |

### Result Sentinels

| Flag | Meaning |
|---|---|
| `ITEM_NOT_AVAILABLE` | Key does not exist (appears in `actual_etag`, `resulting_etag`, or `new_value`) |
| `VALUE_NOT_RETRIEVED` | Value exists but wasn't fetched (IO optimization) |

---

## Result Types

All conditional operations return structured results with full state information, allowing callers to make decisions without extra round-trips.

### `ConditionalOperationResult` (frozen dataclass)

Returned by `get_item_if`, `set_item_if`, `setdefault_if`, `discard_if`.

| Field | Type | Description |
|---|---|---|
| `condition_was_satisfied` | `bool` | Did the ETag check pass? |
| `actual_etag` | `ETagValue \| ITEM_NOT_AVAILABLE` | ETag at check time |
| `resulting_etag` | `ETagValue \| ITEM_NOT_AVAILABLE` | ETag after the operation |
| `new_value` | `Value \| ITEM_NOT_AVAILABLE \| VALUE_NOT_RETRIEVED` | Value after the operation |

**Four canonical result patterns:**

| Pattern | `condition_was_satisfied` | `resulting_etag` | `new_value` |
|---|---|---|---|
| Item not available | varies | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` |
| Unchanged (no mutation) | varies | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` |
| Write success | `True` | new ETag | written value |
| Delete success | `True` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` |

### `OperationResult` (frozen dataclass)

Returned by `transform_item`. Since `transform_item` handles retries internally and is unconditional from the caller's perspective, the result omits condition-related fields.

| Field | Type | Description |
|---|---|---|
| `resulting_etag` | `ETagValue \| ITEM_NOT_AVAILABLE` | ETag after the operation |
| `new_value` | `Value \| ITEM_NOT_AVAILABLE` | Value after the operation |

---

## S3 Implementation Details

`BasicS3Dict` achieves true atomicity by mapping persidict conditions directly to S3 conditional request headers.

### Condition-to-header mapping

| Condition | expected_etag | S3 Header |
|---|---|---|
| `ETAG_IS_THE_SAME` | real ETag | `IfMatch: <etag>` |
| `ETAG_IS_THE_SAME` | `ITEM_NOT_AVAILABLE` | `IfNoneMatch: *` (insert-if-absent) |
| `ETAG_HAS_CHANGED` | real ETag | `IfNoneMatch: <etag>` |
| `ETAG_HAS_CHANGED` | `ITEM_NOT_AVAILABLE` | `IfMatch: <actual_etag>` (from HEAD) |
| `ANY_ETAG` | any | (no headers) |

### Round-trip optimization

- **Fast path:** `set_item_if` with `ETAG_IS_THE_SAME` and a known ETag executes as a single conditional PUT.
- **Fallback path:** Other conditions issue a HEAD to get the actual ETag, then a conditional PUT/DELETE.
- When S3 returns 409/412 (condition failed), persidict re-reads the current state and returns a structured failure result.

---

## Caching Layer Behavior

Persidict's caching layers delegate conditional operations to the underlying backend and update caches as side effects:

| Wrapper | Conditional ops delegation | Cache sync |
|---|---|---|
| `MutableDictCached` | Delegates all to `main_dict` | Updates caches on successful writes; `get_item_if` refreshes caches whenever it retrieves a value (even on condition failure) |
| `AppendOnlyDictCached` | Delegates `get_item_if`/`set_item_if` | Caches values on successful writes; `get_item_if` caches retrieved values on miss |
| `S3Dict_FileDirCached` | Wires `BasicS3Dict` + `FileDirDict` caches | Via `MutableDictCached` or `AppendOnlyDictCached` |

---

## Usage Patterns

These patterns demonstrate how to use persidict's conditional operations in practice.

### Optimistic concurrency (compare-and-swap loop)
```python
while True:
    r = d.get_item_if(k, condition=ANY_ETAG, expected_etag=ITEM_NOT_AVAILABLE)
    new_val = compute(r.new_value)
    r2 = d.set_item_if(k, value=new_val, condition=ETAG_IS_THE_SAME, expected_etag=r.actual_etag)
    if r2.condition_was_satisfied:
        break  # success
    # else: conflict, loop retries with fresh state
```

### Insert-if-absent (one-shot)
```python
r = d.setdefault_if(k, default_value=val, condition=ETAG_IS_THE_SAME, expected_etag=ITEM_NOT_AVAILABLE)
```

### Conditional GET (IO optimization)
```python
# Only fetch the value if it changed since we last saw it.
# If unchanged, r.new_value is VALUE_NOT_RETRIEVED — use your cached copy.
r = d.get_item_if(k, condition=ETAG_HAS_CHANGED, expected_etag=cached_etag, retrieve_value=IF_ETAG_CHANGED)
if r.condition_was_satisfied:
    cached_value = r.new_value       # fresh data
    cached_etag = r.resulting_etag   # remember for next check
```

### Delete only a known version
```python
r = d.discard_if(k, condition=ETAG_IS_THE_SAME, expected_etag=known_etag)
```

---

## Failure Modes & Recovery

Persidict conditional operations fail gracefully, returning structured results instead of raising exceptions for condition mismatches. Understanding the failure scenarios helps you write correct retry logic.

### F1. ETag mismatch (condition not satisfied)

- **Trigger:** The item's current ETag does not satisfy the requested condition. Another process wrote or deleted the item between the caller's read and conditional write.
- **Applies to:** `set_item_if`, `setdefault_if`, `discard_if`, `get_item_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag` reflects the current state. `resulting_etag == actual_etag` (no mutation). `new_value` is the existing value (if `retrieve_value=ALWAYS_RETRIEVE`), `VALUE_NOT_RETRIEVED` (if `NEVER_RETRIEVE` or `IF_ETAG_CHANGED`), or `ITEM_NOT_AVAILABLE` (if the key is absent).
- **Recovery:** Re-read with `get_item_if` using `ANY_ETAG` to get the fresh value and ETag, then retry the conditional write. This is the standard optimistic-concurrency retry loop.

### F2. Key disappeared between read and write

- **Trigger:** The caller read an item and its ETag, but by the time the conditional write executes, the key no longer exists (another process deleted it).
- **Applies to:** `set_item_if`, `discard_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag=ITEM_NOT_AVAILABLE`. `resulting_etag=ITEM_NOT_AVAILABLE`. `new_value=ITEM_NOT_AVAILABLE`.
- **Recovery:** The caller can choose to re-insert (via `set_item_if` with `expected=ITEM_NOT_AVAILABLE`) or treat the deletion as authoritative and stop.

### F3. Key appeared between read and write (insert-if-absent race)

- **Trigger:** The caller intended to insert a new key (`expected=ITEM_NOT_AVAILABLE`, `condition=ETAG_IS_THE_SAME`), but another process inserted it first.
- **Applies to:** `set_item_if`, `setdefault_if`.
- **Result fields:** `condition_was_satisfied=False`. `actual_etag` is the ETag of the newly inserted item. `new_value` is the existing value (if retrieved). For `setdefault_if`, the existing value is returned without mutation regardless of condition outcome.
- **Recovery:** Accept the existing value, or re-read and retry with the new ETag if the caller needs to overwrite.

### F4. `ConcurrencyConflictError` (retries exhausted)

- **Trigger:** `transform_item` encountered ETag conflicts on every attempt and exhausted `n_retries`.
- **Applies to:** `transform_item` only.
- **Exception:** `ConcurrencyConflictError(key, attempts)` is raised (not a result — this is the only failure mode that raises).
- **Recovery:** The caller can retry with a higher `n_retries`, use `n_retries=None` for unbounded retries, or fall back to an explicit `get_item_if` + `set_item_if` loop with custom backoff. Persistent conflicts suggest high contention — consider redesigning the key space or using external coordination.

### F5. S3 conditional request failure (412/409)

- **Trigger:** S3 returns HTTP 412 Precondition Failed or 409 Conflict because the conditional header (`IfMatch`, `IfNoneMatch`) did not match the object's current ETag.
- **Applies to:** `BasicS3Dict` internal handling of `set_item_if`, `setdefault_if`, `discard_if`.
- **Visible effect:** The S3 error is caught internally and translated into a `ConditionalOperationResult` with `condition_was_satisfied=False`. The caller never sees the `ClientError` — it is the same as F1.
- **Recovery:** Same as F1.

### F6. Weak ETag collision (false match)

- **Trigger:** Two different values produce the same ETag. This can happen with stat-derived ETags on `FileDirDict` (coarse mtime resolution, though inode inclusion mitigates this for atomic replacements), and has a non-zero theoretical probability even for S3-native ETags. `LocalDict` uses a monotonic counter and is not susceptible to this failure mode within a single process.
- **Applies to:** All backends, but practically significant only for `FileDirDict`.
- **Visible effect:** `ETAG_IS_THE_SAME` incorrectly reports that the item has not changed. A conditional write may succeed when it should have failed, or `ETAG_HAS_CHANGED` may fail when the item actually did change.
- **Recovery:** No programmatic recovery. This is a known limitation documented in R1. Callers who require strong consistency must use `BasicS3Dict` or add external coordination.

### Failure identification quick reference

| Scenario | `condition_was_satisfied` | `actual_etag` | `resulting_etag` | `new_value` | Raises? |
|---|---|---|---|---|---|
| F1: ETag mismatch (key exists) | `False` | current ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F2: Key disappeared | `False` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` | No |
| F3: Key appeared (insert race) | `False` | new ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F4: Retries exhausted | N/A | N/A | N/A | N/A | `ConcurrencyConflictError` |
| F5: S3 412/409 | `False` | re-read ETag | `== actual_etag` | existing value or `VALUE_NOT_RETRIEVED` | No |
| F6: Weak ETag collision | `True` (incorrect) | stale ETag | may change | may be wrong value | No |

### Recovery strategies summary

- **ETag mismatch / Key disappeared / Key appeared / S3 412/409:** Re-read with `get_item_if(..., ANY_ETAG)` to get the fresh value and ETag, then retry the conditional write. This is the standard optimistic-concurrency loop.
- **Retries exhausted:** Increase `n_retries`, use `n_retries=None` for unbounded retries, or fall back to an explicit `get_item_if` + `set_item_if` loop with custom backoff. Persistent conflicts suggest high contention on the key.
- **Weak ETag collision:** No programmatic recovery. Use `BasicS3Dict` for strong consistency, or add external coordination for file-based backends.

---

## Design Rationale

Key design choices that shape persidict's conditional operations:

- **Singletons for sentinels** — `ITEM_NOT_AVAILABLE`, `KEEP_CURRENT`, etc. are singleton instances, enabling fast `is` identity checks instead of value comparisons.
- **Frozen dataclass results** — `ConditionalOperationResult` and `OperationResult` are immutable, preventing accidental mutation of returned state and making them safe to cache.
- **Uniform absent-key representation** — A single sentinel (`ITEM_NOT_AVAILABLE`) is used in all positions (expected_etag, actual_etag, resulting_etag, new_value, transformer input), eliminating null/None ambiguity and simplifying condition logic.
