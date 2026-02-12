# Persidict ETag & Conditional Operations — API & Implementation Cheatsheet

## Types at a Glance

### ETag Values

| Type | Meaning |
|---|---|
| `ETagValue` (`str`) | Opaque version identifier for a stored item |
| `ITEM_NOT_AVAILABLE` | Key is absent (used in place of an ETag when key doesn't exist) |

ETag values are best-effort version identifiers. Collisions are possible
(timestamp-based ETags on `FileDirDict` can repeat, and even S3 ETags have a
non-zero theoretical collision probability). `LocalDict` uses a monotonic
integer counter, which eliminates collisions within a single process. Treat
equality as a strong hint, not an absolute guarantee.

### ETag Conditions

| Flag | Semantics |
|---|---|
| `ANY_ETAG` | Always satisfied (unconditional) |
| `ETAG_IS_THE_SAME` | `expected == actual` |
| `ETAG_HAS_CHANGED` | `expected != actual` |

### Joker Values (in place of real values)

| Flag | Effect when written |
|---|---|
| `KEEP_CURRENT` | No-op; keep existing value unchanged |
| `DELETE_CURRENT` | Delete the key (same as `discard`) |

### Sentinels in Results

| Flag | Meaning |
|---|---|
| `ITEM_NOT_AVAILABLE` | Key does not exist (in `actual_etag`, `resulting_etag`, or `new_value`) |
| `VALUE_NOT_RETRIEVED` | Value exists but wasn't fetched (`retrieve_value=NEVER_RETRIEVE` or `IF_ETAG_CHANGED`) |

---

## Atomicity Matrix

| Operation | `BasicS3Dict` | `FileDirDict` | `LocalDict` | `MutableDictCached` | `AppendOnlyDictCached` |
|---|---|---|---|---|---|
| `get_item_if` | **Atomic** | Best-effort | Best-effort | Delegates | Delegates |
| `set_item_if` | **Atomic** | Best-effort | Best-effort | Delegates | Delegates |
| `setdefault_if` | **Atomic** | Best-effort | Best-effort | Delegates | Delegates |
| `discard_item_if` | **Atomic** | Best-effort | Best-effort | Delegates | N/A |
| `transform_item` | **Atomic** | Best-effort | Best-effort | Delegates | N/A |
| `etag` source | S3-native | `mtime_ns:size` (weak) | Monotonic counter (strong) | Cached | From `main_dict` |

**Atomic** = server-side ETag check + mutation in one request (no TOCTOU).
**Best-effort** = check-then-act; safe for single-process use, but concurrent writers can interleave.
**Delegates** = caching layer forwards to `main_dict`; atomicity depends on the underlying backend.
**N/A** = not supported (`append_only`; raises `TypeError` / `NotImplementedError`).

---

## Result Types

### `ConditionalOperationResult` (frozen dataclass)

Returned by `get_item_if`, `set_item_if`, `setdefault_if`, `discard_item_if`.

| Field | Type | Description |
|---|---|---|
| `condition_was_satisfied` | `bool` | Did the ETag check pass? |
| `requested_condition` | `ETagConditionFlag` | Which condition was requested |
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

Returned by `transform_item` (unconditional).

| Field | Type | Description |
|---|---|---|
| `resulting_etag` | `ETagValue \| ITEM_NOT_AVAILABLE` | ETag after the operation |
| `new_value` | `Value \| ITEM_NOT_AVAILABLE` | Value after the operation |

---

## API Methods

### `get_item_if(key, expected_etag, condition, *, retrieve_value=ALWAYS_RETRIEVE)`

Read-only. Never mutates. `condition_was_satisfied` tells you whether the condition held.

```python
r = d.get_item_if(k, cached_etag, ETAG_HAS_CHANGED, retrieve_value=IF_ETAG_CHANGED)
if r.condition_was_satisfied:
    # ETag changed -> r.new_value has fresh data
else:
    # r.new_value is VALUE_NOT_RETRIEVED -> use your cached copy
```

### `set_item_if(key, value, expected_etag, condition, *, retrieve_value=ALWAYS_RETRIEVE)`

Conditional write. `value` can be a real value, `KEEP_CURRENT`, or `DELETE_CURRENT`. On condition failure: no mutation.

```python
r = d.get_item_if(k, ITEM_NOT_AVAILABLE, ANY_ETAG)
new_val = transform(r.new_value)
r2 = d.set_item_if(k, new_val, r.actual_etag, ETAG_IS_THE_SAME)
if not r2.condition_was_satisfied:
    pass  # conflict — retry
```

### `setdefault_if(key, default_value, expected_etag, condition, *, retrieve_value=ALWAYS_RETRIEVE)`

Insert-if-absent, guarded by condition. If key exists, returns existing value without mutation regardless of condition.
`default_value` must be a real value (not `KEEP_CURRENT` or `DELETE_CURRENT`).

```python
r = d.setdefault_if(k, initial_value, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)
```

### `discard_item_if(key, expected_etag, condition)`

Conditional delete. No `retrieve_value` parameter — on condition failure, `new_value`
is `VALUE_NOT_RETRIEVED` (unless the key is missing, in which case
`ITEM_NOT_AVAILABLE`); on success, `new_value` is `ITEM_NOT_AVAILABLE`.

```python
r = d.discard_item_if(k, known_etag, ETAG_IS_THE_SAME)
```

### `transform_item(key, transformer, *, n_retries=6)`

Unconditional read-modify-write implemented as a conditional-ops retry loop. `transformer` receives current value (or `ITEM_NOT_AVAILABLE`) and returns new value, `KEEP_CURRENT`, or `DELETE_CURRENT`.
Atomic only when the backend's conditional operations are atomic; on persistent conflicts it raises `TransformConflictError`. The transformer may be called multiple times under contention.

```python
def increment(v):
    if v is ITEM_NOT_AVAILABLE:
        return 1
    return v + 1

r = d.transform_item(k, increment)
```

---

## Condition Logic Truth Table

| Condition | `expected == actual` | `expected != actual` |
|---|---|---|
| `ANY_ETAG` | **satisfied** | **satisfied** |
| `ETAG_IS_THE_SAME` | **satisfied** | not satisfied |
| `ETAG_HAS_CHANGED` | not satisfied | **satisfied** |

`ITEM_NOT_AVAILABLE == ITEM_NOT_AVAILABLE` is `True`, so `ETAG_IS_THE_SAME` with `expected=ITEM_NOT_AVAILABLE` passes when the key is absent.

---

## ETag Implementations by Backend

| Backend | `etag()` source | Conditional ops atomicity |
|---|---|---|
| `BasicS3Dict` | S3 native ETag (`head_object`) | Atomic (S3 conditional headers) |
| `FileDirDict` | `mtime_ns:file_size` (timestamp-based, weak) | Non-atomic (check-then-act)\* |
| `LocalDict` | Monotonic write counter (integer, strong) | Non-atomic (check-then-act) |

\* `FileDirDict` intentionally avoids OS-native file locking because it must work with shared folders synced via Dropbox (and similar services), where advisory/mandatory locks are not reliably propagated across machines.

---

## S3 Atomicity (BasicS3Dict)

### Condition-to-header mapping

| Condition | expected_etag | S3 Header |
|---|---|---|
| `ETAG_IS_THE_SAME` | real ETag | `IfMatch: <etag>` |
| `ETAG_IS_THE_SAME` | `ITEM_NOT_AVAILABLE` | `IfNoneMatch: *` (insert-if-absent) |
| `ETAG_HAS_CHANGED` | real ETag | `IfNoneMatch: <etag>` |
| `ETAG_HAS_CHANGED` | `ITEM_NOT_AVAILABLE` | `IfMatch: <actual_etag>` (from HEAD) |
| `ANY_ETAG` | any | (no headers) |

### Round-trip strategies

- **Fast path:** `set_item_if` with `ETAG_IS_THE_SAME` + real value — single S3 PUT with conditional headers.
- **Fallback path:** Other conditions — HEAD to get actual ETag, then conditional PUT/DELETE.
- On S3 409/412 (condition failed): re-reads current state and returns a failure result.

---

## Caching Layer Behavior

| Wrapper | Conditional ops delegation | Cache sync |
|---|---|---|
| `MutableDictCached` | Delegates all to `main_dict` | Updates caches on successful writes; `get_item_if` refreshes caches whenever it retrieves a value (even on condition failure) |
| `AppendOnlyDictCached` | Delegates `get_item_if`/`set_item_if` | Caches values on successful writes; `get_item_if` caches retrieved values on miss |
| `S3Dict_FileDirCached` | Wires `BasicS3Dict` + `FileDirDict` caches | Via `MutableDictCached` or `AppendOnlyDictCached` |

---

## Common Patterns

### Optimistic concurrency (compare-and-swap loop)
```python
while True:
    r = d.get_item_if(k, ITEM_NOT_AVAILABLE, ANY_ETAG)
    new_val = compute(r.new_value)
    r2 = d.set_item_if(k, new_val, r.actual_etag, ETAG_IS_THE_SAME)
    if r2.condition_was_satisfied:
        break  # success
    # else: conflict, loop retries with fresh state
```

### Insert-if-absent (one-shot)
```python
r = d.setdefault_if(k, val, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)
```

### Conditional GET (bandwidth optimization)
```python
r = d.get_item_if(k, cached_etag, ETAG_HAS_CHANGED, retrieve_value=IF_ETAG_CHANGED)
if not r.condition_was_satisfied:
    pass  # cache is still valid, r.new_value == VALUE_NOT_RETRIEVED
```

### Delete only a known version
```python
r = d.discard_item_if(k, known_etag, ETAG_IS_THE_SAME)
```

---

## Failure Modes Quick Reference

Every conditional operation returns a result rather than raising on condition mismatch. The table below maps each failure scenario to the result fields the caller should inspect.

| Failure | Trigger | `condition_was_satisfied` | `actual_etag` | `new_value` | Raises? |
|---|---|---|---|---|---|
| **ETag mismatch** | Another writer changed the item | `False` | current ETag | existing value or `VALUE_NOT_RETRIEVED` | No |
| **Key disappeared** | Item deleted between read and write | `False` | `ITEM_NOT_AVAILABLE` | `ITEM_NOT_AVAILABLE` | No |
| **Key appeared** | Insert-if-absent race lost | `False` | new ETag | existing value or `VALUE_NOT_RETRIEVED` | No |
| **Retries exhausted** | `transform_item` hit `n_retries` conflicts | N/A | N/A | N/A | `TransformConflictError` |
| **S3 412/409** | S3 conditional header mismatch | `False` | re-read ETag | existing value or `VALUE_NOT_RETRIEVED` | No |
| **Weak ETag collision** | Different values, same ETag (timestamp-based on `FileDirDict`) | `True` (incorrect) | stale ETag | may be wrong value | No |

### Recovery strategies

- **ETag mismatch / Key disappeared / Key appeared / S3 412/409:** Re-read with `get_item_if(..., ANY_ETAG)` to get the fresh value and ETag, then retry the conditional write. This is the standard optimistic-concurrency loop.
- **Retries exhausted:** Increase `n_retries`, use `n_retries=None` for unbounded retries, or fall back to an explicit `get_item_if` + `set_item_if` loop with custom backoff. Persistent conflicts suggest high contention on the key.
- **Weak ETag collision:** No programmatic recovery. Use `BasicS3Dict` for strong consistency, or add external coordination for file-based backends.
