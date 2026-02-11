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

## Requirements

### R1. Every item has a version identifier (ETag)

- `etag(key)` returns an opaque string intended to change when the item's value changes.
- If the key does not exist, `etag()` raises `KeyError`.
- ETag stability: calling `etag()` twice without an intervening write returns the same value.
- ETag is opaque; do not assume a new ETag after a write if the value is identical.

**Weak semantics note:** ETags are best-effort version identifiers, not a
cryptographic guarantee. Some backends use timestamp-derived ETags, which can
repeat under coarse clock/filesystem resolution, and even native S3 ETags have
a non-zero theoretical collision probability. Treat ETag matches as a strong
hint, not absolute proof of identity.

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

- `always_retrieve_value=False` allows skipping value retrieval when the caller already has a cached copy and only wants to know if the ETag changed.
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

## Design Decisions

- **Singletons for sentinels** — `ITEM_NOT_AVAILABLE`, `KEEP_CURRENT`, etc. are singleton instances, enabling fast `is` identity checks.
- **Frozen dataclass results** — `ConditionalOperationResult` and `OperationResult` are immutable, preventing accidental mutation of returned state.
- **Uniform absent-key representation** — a single sentinel (`ITEM_NOT_AVAILABLE`) is used in all positions (expected_etag, actual_etag, resulting_etag, new_value, transformer input), avoiding null/None ambiguity.
