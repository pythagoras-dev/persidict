# Core Design Principles of `persidict`

`persidict` is built on a set of core assumptions and design trade-offs that make it well-suited for distributed computing environments. Understanding these principles is key to using the library effectively.

## 1. Familiar `dict`-like API

At its core, `persidict` is designed to be intuitive for Python developers by mirroring the standard `dict` interface. You can use common operations like `__getitem__`, `__setitem__`, and `in` just as you would with a regular dictionary. However, this familiar API is extended with additional methods like `timestamp()`, `random_key()`, and `get_subdict()` that provide functionality essential for persistent and distributed data stores.

## 2. Built for Distributed Systems with Optimistic Concurrency

The library is fundamentally designed for concurrent access from multiple processes or machines. It uses an optimistic concurrency model, which assumes that conflicts are rare but provides mechanisms to handle them:

* **Last Write Wins**: For mutable data, the last write operation to a key will overwrite any previous value. This is a simple yet effective strategy for many distributed applications.
* **Atomic Writes**: Operations on a single key-value pair are atomic. `FileDirDict` uses atomic `os.replace` to prevent data corruption from partial writes, while S3 provides its own guarantees for object-level atomicity. Note that `persidict` does not support multi-key atomic transactions.
* **ETag-based Consistency**: For cloud storage, `S3Dict` and its caching layers use ETags for conditional requests. This minimizes unnecessary data transfer by only fetching objects if they have changed on the server, ensuring efficient and consistent reads. For other storage backends, `persidict` uses timestamps in lieu of ETags.

## 3. Pluggable and Cloud-Ready Storage Backends

`persidict` provides a unified API over different storage mechanisms, allowing you to switch between them with minimal code changes. The primary backends are:

* `FileDirDict`: For local filesystem storage, ideal for single-machine applications or development and testing.
* `S3Dict`: For cloud-based storage on AWS S3, designed for scalable and distributed applications.
* `LocalDict`: An in-memory implementation perfect for testing and ephemeral data.

## 4. Hierarchical and Filesystem-Safe Keys

Keys in `persidict` are more than just strings; they are a core design feature:

* **Hierarchical Structure**: Keys are sequences of strings (`SafeStrTuple`), which creates a natural directory-like hierarchy in the storage backend. This allows for logical data organization and enables powerful features like `get_subdict()`.
* **Safety and Portability**: The `SafeStrTuple` class enforces that all key components use a restricted set of URL- and filename-safe characters. Additionally, `FileDirDict` can add a hash suffix to key components (`digest_len`) to prevent collisions on case-insensitive filesystems, ensuring that your keys are portable across different operating systems.

## 5. Flexible Serialization and Type Safety

To accommodate a wide range of use cases, `persidict` offers flexibility in how data is stored:

* **Multiple Formats**: You can choose the serialization type via the `serialization_format` parameter. The options are `pkl` (for any Python object), `json` (for human-readable data), and plain text.
* **Optional Type Safety**: For applications that require strict data validation, you can enforce that all values are instances of a specific class by setting the `base_class_for_values` parameter.

## 6. Layered Architecture and Composition

`persidict` follows a layered architecture pattern that enables powerful composition of functionality:

* **Base Layer**: Core dictionary implementations (`FileDirDict`, `S3Dict`, `LocalDict`) provide the fundamental storage operations
* **Caching Layer**: `CachedMutableDict` and `CachedAppendOnlyDict` add intelligent caching on top of any base dictionary
* **Behavioral Layer**: Specialized dictionaries like `WriteOnceDict` and `EmptyDict` modify behavior through composition
* **Multi-Dictionary Layer**: `OverlappingMultiDict` enables working with multiple dictionaries as a single unified interface

This design allows you to stack different capabilities (e.g., S3 storage + caching + write-once semantics) by composing different dictionary classes.

## 7. Performance Through Intelligent Caching

The library includes sophisticated caching strategies optimized for different use patterns:

* **Mutable Caching**: `CachedMutableDict` uses ETags/timestamps to validate cache entries, ensuring consistency while minimizing I/O
* **Append-Only Caching**: `CachedAppendOnlyDict` can cache more aggressively since immutable data never changes, leading to significant performance gains
* **Lazy Loading**: Data is only loaded from persistent storage when actually accessed, reducing memory footprint and startup time

## Trade-offs and Limitations

Understanding these design choices helps set appropriate expectations:

* **Eventual Consistency**: The optimistic concurrency model means you may see stale data briefly in distributed scenarios
* **No Transactions**: Multi-key atomic operations are not supported to maintain simplicity
* **Memory Usage**: Caching layers trade memory for performance, memory is generally assumed to be unlimited
* **Network Dependency**: Cloud-based storage backends require reliable network connectivity

## Choosing the Right Configuration

The layered design means you need to choose the right combination for your use case:

* **Development/Testing**: `LocalDict` or `FileDirDict` for simplicity
* **Production Single-Machine**: `CachedMutableDict(FileDirDict(...))` for performance
* **Production Distributed**: `CachedMutableDict(S3Dict(...))` for scalability
* **Append-Only Workloads**: `CachedAppendOnlyDict` for maximum performance
* **Content Addressing Scenarios**: `WriteOnceDict` to avoid redundant writes