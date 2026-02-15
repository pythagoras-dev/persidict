# Core Design Principles of `persidict`

`persidict` is built on a small set of explicit assumptions and trade‑offs that make it well‑suited for distributed computing. Understanding these principles will help you use the library effectively.

## 1. Familiar, `dict`‑like API 

`persidict` mirrors Python’s built‑in `dict` interface for familiar access (`__getitem__`, `__setitem__`, `__contains__`), but stores values durably and doesn’t guarantee insertion order. It also adds helpers like `timestamp()`, `random_key()`, and `get_subdict()`.

## 2. Built for distributed use with optimistic concurrency

Concurrent access from multiple processes or machines is a first‑class use case. `persidict` follows an optimistic concurrency model, assuming conflicts are rare, while providing tools to handle them:

- Last‑write‑wins: For mutable data, the last write to a key overwrites prior values. This simple strategy works well for many distributed workloads.
- Atomic single‑key operations: `FileDirDict` uses atomic `os.replace` to avoid partial writes, while S3 provides its own guarantees for object-level atomicity. Multi‑key atomic transactions are not supported.
- ETag/timestamp validation: S3 uses native ETags (via `BasicS3Dict`/`S3Dict`); local backends derive ETags (`FileDirDict` mtime+size+inode, `LocalDict` monotonic counter, base `PersiDict` timestamp string) for conditional ops and cache validation.

## 3. Conditional operations are first‑class

Conditional operations are the primary mechanism for avoiding lost updates. Instead of implicit compare‑and‑swap, the API makes the condition explicit and returns structured results:

- Conditional reads and writes: `get_item_if`, `set_item_if`, `setdefault_if`, `discard_item_if`, and the retrying `transform_item`.
- Explicit conditions and absent‑key handling: `ANY_ETAG`, `ETAG_IS_THE_SAME`, `ETAG_HAS_CHANGED`, with `ITEM_NOT_AVAILABLE` standing in for missing keys.
- Joker values and full results: `KEEP_CURRENT` and `DELETE_CURRENT` allow no‑op and delete in write paths; results include actual and resulting ETags plus the resulting value.
- Backend atomicity is explicit: `BasicS3Dict` uses S3 conditional headers for atomicity, while `FileDirDict` uses check‑then‑act to remain compatible with shared folders that do not propagate locks.

For the full contract, see [`etag_conditional_ops_requirements.md`](https://github.com/pythagoras-dev/persidict/blob/master/etag_conditional_ops_requirements.md).

## 4. Pluggable, cloud‑ready storage backends

A unified API spans multiple storage mechanisms, so you can switch backends with minimal code changes. Primary backends include:

- `FileDirDict`: Local filesystem storage; good for single‑machine apps, development, and sync‑based folders (e.g., Dropbox).
- `BasicS3Dict`: Direct S3 storage with no local cache.
- `S3Dict`: Cached S3 (`BasicS3Dict` + local `FileDirDict` cache) for faster reads and ETag‑based validation.
- `LocalDict`: In‑memory storage for testing and ephemeral data.

## 5. Hierarchical, filesystem-safe keys

Keys are a core design feature, not just strings:

- Hierarchical paths: Keys are sequences (`SafeStrTuple`) that form a natural namespace and enable `get_subdict()`.
- Safe by construction: Components are constrained to a small ASCII-safe set, length-bounded, and exclude special path segments like `.`/`..`, so the same key works across filesystems and URLs.
- Collision-resistant: `FileDirDict` can append a deterministic digest suffix (`digest_len`) to avoid case-insensitive collisions while keeping keys stable across platforms.

## 6. Flexible serialization and optional type safety

Storage format and type safety are configurable:

- Multiple formats: `pkl` uses joblib for arbitrary Python objects; `json` uses jsonpickle for human‑readable (but not strict JSON) storage; other formats are treated as plain text.
- Optional type safety: Enforce a base class via `base_class_for_values`; for non‑string values, formats are limited to `pkl`/`json`.

## 7. Layered architecture and composition

Capabilities are composed in layers:

- Base layer: Core implementations (`FileDirDict`, `BasicS3Dict`, `LocalDict`) provide fundamental storage operations.
- Caching layer: `MutableDictCached` validates cached entries via ETags; `AppendOnlyDictCached` trusts cached values once written.
- Behavioral layer: `WriteOnceDict` ignores repeat writes (with optional checks); `EmptyDict` accepts writes but discards them.
- Multi‑dictionary layer: `OverlappingMultiDict` exposes multiple serialization formats as attributes rather than a single dict.

This design lets you stack capabilities (e.g., S3 storage + caching + write‑once semantics) by composing classes.

## 8. Performance through intelligent caching

Caching strategies can be tuned for different access patterns:

- Mutable caching: `MutableDictCached` checks ETags/timestamps on reads to keep caches consistent with the source of truth.
- Append‑only caching: `AppendOnlyDictCached` can return cached values without validation because items never change.
- Read‑through behavior: Reads populate the cache on misses; writes go to the main store then mirror into cache.

## 9. API conventions and typing

The public API is designed to be readable and safe:

- Keyword‑only by default for constructors and conditional operations; standard dict methods keep familiar positional arguments.
- Type‑hint‑first: all public APIs are fully typed with modern Python syntax and generics for static checking; runtime enforcement uses `base_class_for_values`. See [`type_hints.md`](https://github.com/pythagoras-dev/persidict/blob/master/type_hints.md).

## 10. Trade‑offs and limitations

These choices come with explicit trade‑offs:

- Eventual consistency: Under optimistic concurrency you may briefly see stale data in distributed scenarios.
- No transactions: Multi‑key atomic operations are intentionally not supported to keep the model simple.
- Memory vs. speed: Caching trades memory for performance; memory is generally assumed plentiful at the application level.
- Network dependency: Cloud backends depend on reliable network connectivity.
- Locking trade‑off: OS‑native file locking is avoided to remain compatible with cross‑platform filesystems and sync services (e.g., Dropbox).

## 11. Choosing the right configuration

Pick a composition that matches your use case:

- Development/testing: `LocalDict` or `FileDirDict` for simplicity.
- Production, single machine: `MutableDictCached(FileDirDict(...))` for performance.
- Production, distributed: `MutableDictCached(BasicS3Dict(...))` for scalability.
- Append‑only workloads: `AppendOnlyDictCached` for maximum performance.
- Content‑addressed scenarios: `WriteOnceDict` to avoid redundant writes.
