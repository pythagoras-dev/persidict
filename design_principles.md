# Core Design Principles of `persidict`

`persidict` is built on a small set of explicit assumptions and trade‑offs that make it well‑suited for distributed computing. Understanding these principles will help you use the library effectively.

## 1. Familiar, `dict`‑like API

`persidict` mirrors Python’s built‑in `dict` interface, so common operations like `__getitem__`, `__setitem__`, and `__contains__` behave as you expect. The API is then extended with capabilities useful for persistence and distribution, such as `timestamp()`, `random_key()`, and `get_subdict()`.

## 2. Built for distributed use with optimistic concurrency

Concurrent access from multiple processes or machines is a first‑class use case. `persidict` follows an optimistic concurrency model, assuming conflicts are rare, while providing tools to handle them:

- Last‑write‑wins: For mutable data, the last write to a key overwrites prior values. This simple strategy works well for many distributed workloads.
- Atomic single‑key operations: `FileDirDict` uses atomic `os.replace` to avoid partial writes, while S3 provides its own guarantees for object-level atomicity. Multi‑key atomic transactions are not supported.
- ETag/timestamp validation: For cloud storage, `S3Dict` and caching layers leverage ETags for conditional requests to minimize transfer and ensure efficient, consistent reads. For other backends, timestamps are used in lieu of ETags.

## 3. Pluggable, cloud‑ready storage backends

A unified API spans multiple storage mechanisms, so you can switch backends with minimal code changes. Primary backends include:

- `FileDirDict`: Local filesystem storage; great for single‑machine apps, development, and testing.
- `S3Dict`: AWS S3‑backed storage for scalable, distributed applications.
- `LocalDict`: In‑memory storage for testing and ephemeral data.

## 4. Hierarchical, filesystem‑safe keys

Keys are a core design feature, not just strings:

- Hierarchical structure: Keys are sequences of safe strings (`SafeStrTuple`), forming a natural directory‑like hierarchy and enabling features like `get_subdict()`.
- Safety and portability: `SafeStrTuple` restricts components to URL/filename‑safe characters. `FileDirDict` can also add a hash suffix (`digest_len`) to prevent collisions on case‑insensitive filesystems, keeping keys portable across OSes.

## 5. Flexible serialization and optional type safety

Storage format and type safety are configurable:

- Multiple formats: Choose via `serialization_format`: `pkl` (any Python object), `json` (human‑readable), or plain text.
- Optional type safety: Enforce that values are instances of a specific class with `base_class_for_values` when strict validation is required.

## 6. Layered architecture and composition

Capabilities are composed in layers:

- Base layer: Core implementations (`FileDirDict`, `S3Dict`, `LocalDict`) provide fundamental storage operations.
- Caching layer: `MutableDictCached` and `AppendOnlyDictCached` add intelligent caching atop any base dictionary.
- Behavioral layer: Wrappers like `WriteOnceDict` and `EmptyDict` modify behavior via composition.
- Multi‑dictionary layer: `OverlappingMultiDict` lets you work with multiple dictionaries through a single interface.

This design lets you stack capabilities (e.g., S3 storage + caching + write‑once semantics) by composing classes.

## 7. Performance through intelligent caching

Caching strategies can be tuned for different access patterns:

- Mutable caching: `MutableDictCached` uses ETags/timestamps to validate cache entries and minimize I/O.
- Append‑only caching: `AppendOnlyDictCached` can cache more aggressively because immutable data never changes, yielding substantial speedups.
- Lazy loading: Data is fetched from persistent storage only when accessed, reducing memory footprint and startup time.

## Trade‑offs and limitations

These choices come with explicit trade‑offs:

- Eventual consistency: Under optimistic concurrency you may briefly see stale data in distributed scenarios.
- No transactions: Multi‑key atomic operations are intentionally not supported to keep the model simple.
- Memory vs. speed: Caching trades memory for performance; memory is generally assumed plentiful at the application level.
- Network dependency: Cloud backends depend on reliable network connectivity.

## Choosing the right configuration

Pick a composition that matches your use case:

- Development/testing: `LocalDict` or `FileDirDict` for simplicity.
- Production, single machine: `MutableDictCached(FileDirDict(...))` for performance.
- Production, distributed: `MutableDictCached(BasicS3Dict(...))` for scalability.
- Append‑only workloads: `AppendOnlyDictCached` for maximum performance.
- Content‑addressed scenarios: `WriteOnceDict` to avoid redundant writes.