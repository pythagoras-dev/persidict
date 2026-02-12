# persidict

[![PyPI version](https://img.shields.io/pypi/v/persidict.svg?color=green)](https://pypi.org/project/persidict/)
[![Python versions](https://img.shields.io/pypi/pyversions/persidict.svg)](https://github.com/pythagoras-dev/persidict)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/pythagoras-dev/persidict/blob/master/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/persidict?color=blue)](https://pypistats.org/packages/persidict)
[![Documentation Status](https://app.readthedocs.org/projects/persidict/badge/?version=latest)](https://persidict.readthedocs.io/en/latest/)
[![Code style: pep8](https://img.shields.io/badge/code_style-pep8-blue.svg)](https://peps.python.org/pep-0008/)
[![Docstring Style: Google](https://img.shields.io/badge/docstrings_style-Google-blue)](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
[![Ruff](https://github.com/pythagoras-dev/persidict/actions/workflows/ruff.yml/badge.svg?branch=master)](https://github.com/pythagoras-dev/persidict/actions/workflows/ruff.yml)

Simple persistent dictionaries for distributed applications in Python.

## What Is It?

`persidict` is a lightweight persistent key-value store for Python. 
It saves a dictionary to either a local directory or an AWS S3 bucket, 
storing each value as its own file or S3 object. Keys are limited to 
URL/filename-safe strings or sequences of strings.

In contrast to traditional persistent dictionaries (e.g., Python's `shelve`), 
`persidict` is [designed](https://github.com/pythagoras-dev/persidict/blob/master/design_principles.md) 
for distributed environments where multiple processes 
on different machines concurrently work with the same store.

## Why Use It?

A small API surface with scalable storage backends and explicit concurrency controls.

### Features

* **Persistent Storage**: Save dictionaries to the local filesystem 
(`FileDirDict`) or AWS S3 (`S3Dict`).
* **Standard Dictionary API**: Use `PersiDict` objects like standard 
Python dictionaries (`__getitem__`, `__setitem__`, `__delitem__`, 
`keys`, `values`, `items`).
* **Distributed Computing Ready**: Designed for concurrent access 
in distributed environments.
* **Flexible Serialization**: Store values as pickles (`pkl`), 
JSON (`json`), or plain text.
* **Type Safety**: Optionally enforce that all values in a dictionary are
instances of a specific class.
* **Generic Type Parameters**: Use `FileDirDict[MyClass]` for static type
checking with mypy/pyright.
* **Advanced Functionality**: Includes features like write-once dictionaries, 
timestamping of entries, and tools for handling filesystem-safe keys.
* **ETag-Based Conditional Operations**: Optimistic concurrency helpers for
conditional reads, writes, deletes, and transforms based on per-key ETags.
* **Hierarchical Keys**: Keys can be sequences of strings, 
creating a directory-like structure within the storage backend.

### Use Cases

`persidict` is well-suited for a variety of applications, including:

* **Caching**: Store results of expensive computations and retrieve them later, 
even across different machines.
* **Configuration Management**: Manage application settings 
in a distributed environment, allowing for easy updates and access.
* **Data Pipelines**: Share data between different stages 
of a data processing pipeline.
* **Distributed Task Queues**: Store task definitions and results 
in a shared location.
* **Memoization**: Cache function call results 
in a persistent and distributed manner.

## Usage

### Storing Data on a Local Disk

The `FileDirDict` class saves your dictionary to a local folder. 
Each key-value pair is stored as a separate file.

```python
from persidict import FileDirDict

# Create a dictionary that will be stored in the "my_app_data" folder.
# The folder will be created automatically if it doesn't exist.
app_settings = FileDirDict(base_dir="my_app_data")

# Add and update items just like a regular dictionary.
app_settings["username"] = "alex"
app_settings["theme"] = "dark"
app_settings["notifications_enabled"] = True

# Values can be any pickleable Python object.
app_settings["recent_projects"] = ["project_a", "project_b"]

print(f"Current theme is: {app_settings['theme']}")
# >>> Current theme is: dark

# The data persists!
# If you run the script again or create a new dictionary object
# pointing to the same folder, the data will be there.
reloaded_settings = FileDirDict(base_dir="my_app_data")

print(f"Number of settings: {len(reloaded_settings)}")
# >>> Number of settings: 4

print("username" in reloaded_settings)
# >>> True
```

### Storing Data in the Cloud (AWS S3)

For distributed applications, you can use **`S3Dict`** to store data in 
an AWS S3 bucket. The usage is identical, allowing you to switch 
between local and cloud storage with minimal code changes.

```python
from persidict import S3Dict

# Create a dictionary that will be stored in an S3 bucket.
# The bucket will be created if it doesn't exist.
cloud_config = S3Dict(bucket_name="my-app-config-bucket")

# Use it just like a FileDirDict.
cloud_config["api_key"] = "ABC-123-XYZ"
cloud_config["timeout_seconds"] = 30

print(f"API Key: {cloud_config['api_key']}")
# >>> API Key: ABC-123-XYZ
```

### Using Type Hints

`persidict` supports two complementary type safety mechanisms:

**Static type checking** with generic parameters (checked by mypy/pyright):

```python
from persidict import FileDirDict

# Create a typed dictionary
d: FileDirDict[int] = FileDirDict(base_dir="./data")
d["count"] = 42
val: int = d["count"]  # Type checker knows this is int

# Works with any PersiDict implementation
from persidict import LocalDict
cache: LocalDict[str] = LocalDict()
```

**Runtime type enforcement** with `base_class_for_values` (checked via isinstance):

```python
d = FileDirDict(base_dir="./data", base_class_for_values=int)
d["count"] = 42      # OK
d["name"] = "Alice"  # Raises TypeError at runtime
```

These mechanisms are kept separate because many type hints cannot be checked
at runtime. For example, `Callable[[int], str]`, `Literal["a", "b"]`,
`TypedDict`, and `NewType` have no `isinstance` equivalent. Use generics for
development-time safety; use `base_class_for_values` when you need runtime validation.

### Conditional Operations

Use conditional operations to avoid lost updates in concurrent scenarios. The
insert-if-absent pattern uses `ITEM_NOT_AVAILABLE` with `ETAG_IS_THE_SAME`.

```python
from persidict import FileDirDict, ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME

d = FileDirDict(base_dir="./data")
r = d.setdefault_if("token", "v1", ITEM_NOT_AVAILABLE, ETAG_IS_THE_SAME)
```

## Comparison With Python Built-in Dictionaries

### Similarities

`PersiDict` subclasses can be used like regular Python dictionaries, supporting: 

* Get, set, and delete operations with square brackets (`[]`).
* Iteration over keys, values, and items.
* Membership testing with `in`.
* Length checking with `len()`.
* Standard methods like `keys()`, `values()`, `items()`, `get()`, `clear()`, `setdefault()`, and `update()`.

### Differences

* **Persistence**: Data is saved between program executions.
* **Keys**: Keys must be URL/filename-safe strings or their sequences.
* **Values**: Values must be serializable in the chosen format (pickle, JSON, or text). You can also constrain values to a specific class.
* **Order**: Insertion order is not preserved.
* **Additional Methods**: `PersiDict` provides extra methods not in the standard dict API, such as `timestamp()`, `etag()`, `random_key()`, `newest_keys()`, `subdicts()`, `discard()`, `get_params()`, and more.
* **Conditional Operations**: ETag-based compare-and-swap reads/writes with
structured results (see [Conditional Operations](#conditional-operations-etag-based)).
* **Special Values**: Use `KEEP_CURRENT` to avoid updating a value 
and `DELETE_CURRENT` to delete a value during a write.

## Glossary

### Core Concepts

* **`PersiDict`**: The abstract base class that defines the common interface 
for all persistent dictionaries in the package. It's the foundation 
upon which everything else is built.
* **`NonEmptyPersiDictKey`**: A type hint that specifies what can be used
as a key in any `PersiDict`. It can be a `NonEmptySafeStrTuple`, a single string, 
or a sequence of strings. When a `PersiDict` method requires a key as an input,
it will accept any of these types and convert them to 
a `NonEmptySafeStrTuple` internally.
* **`NonEmptySafeStrTuple`**: The core data structure for keys. 
It's an immutable, flat tuple of non-empty, URL/filename-safe strings, 
ensuring that keys are consistent and safe for various storage backends. 
When a `PersiDict` method returns a key, it will always be in this format.

### Main Implementations

* **`FileDirDict`**: A primary, concrete implementation of `PersiDict` 
that stores each key-value pair as a separate file in a local directory.
* **`S3Dict`**: The other primary implementation of `PersiDict`, 
which stores each key-value pair as an object in an AWS S3 bucket, 
suitable for distributed environments.

### Key Parameters

* **`serialization_format`**: A key parameter for `FileDirDict` and `S3Dict` that 
determines the serialization format used to store values. 
Common options are `"pkl"` (pickle) and `"json"`. 
Any other value is treated as plain text for string storage.
* **`base_class_for_values`**: An optional parameter for any `PersiDict` 
that enforces type checking on all stored values, ensuring they are 
instances of a specific class.
* **`append_only`**: A boolean parameter that makes items inside a `PersiDict` immutable, 
preventing them from modification or deletion.
* **`digest_len`**: An integer that specifies the length of a hash suffix 
added to key components in `FileDirDict` to prevent collisions 
on case-insensitive file systems.
* **`base_dir`**: A string specifying the directory path where a `FileDirDict`
stores its files. For `S3Dict`, this directory is used to cache files locally.
* **`bucket_name`**: A string specifying the name of the S3 bucket where
an `S3Dict` stores its objects.
* **`region`**: An optional string specifying the AWS region for the S3 bucket.

### Advanced and Supporting Classes

* **`WriteOnceDict`**: A wrapper that enforces write-once behavior 
on any `PersiDict`, ignoring subsequent writes to the same key. 
It also allows for random consistency checks to ensure subsequent 
writes to the same key always match the original value.
* **`OverlappingMultiDict`**: An advanced container that holds 
multiple `PersiDict` instances sharing the same storage 
but with different `serialization_format`s.
* **`LocalDict`**: An in-memory `PersiDict` backed by 
a RAM-only hierarchical store.
* **`EmptyDict`**: A minimal implementation of `PersiDict` that behaves  
like a null device in the OS: accepts all writes, discards them, 
and returns nothing on reads. Always appears empty regardless of 
operations performed on it.

### Special "Joker" Values

* **`Joker`**: The base class for special command-like values that 
can be assigned to a key to trigger an action instead of storing a value.
* **`KEEP_CURRENT`**: A "joker" value that, when assigned to a key, 
ensures the existing value is not changed.
* **`DELETE_CURRENT`**: A "joker" value that deletes the key-value pair 
from the dictionary when assigned to a key.

### ETags and Conditional Flags

* **`ETagValue`**: Opaque per-key version string used for conditional operations.
* **`ETag conditions`**: `ANY_ETAG` (unconditional), `ETAG_IS_THE_SAME` (expected == actual), 
`ETAG_HAS_CHANGED` (expected != actual).
* **`ITEM_NOT_AVAILABLE`**: Sentinel used when a key is missing (stands in for the ETag).
* **`VALUE_NOT_RETRIEVED`**: Sentinel indicating a value exists but was not fetched.

## API Highlights

`PersiDict` subclasses support the standard Python dictionary API, plus these additional methods:

| Method | Return Type | Description |
| :--- | :--- | :--- |
| `timestamp(key)` | `float` | Returns the POSIX timestamp (seconds since epoch) of a key's last modification. |
| `random_key()` | `SafeStrTuple \| None` | Selects and returns a single random key, useful for sampling from the dataset. |
| `oldest_keys(max_n=None)` | `list[SafeStrTuple]` | Returns a list of keys sorted by their modification time, from oldest to newest. |
| `newest_keys(max_n=None)` | `list[SafeStrTuple]` | Returns a list of keys sorted by their modification time, from newest to oldest. |
| `oldest_values(max_n=None)` | `list[Any]` | Returns a list of values corresponding to the oldest keys. |
| `newest_values(max_n=None)` | `list[Any]` | Returns a list of values corresponding to the newest keys. |
| `get_subdict(prefix_key)` | `PersiDict` | Returns a new `PersiDict` instance that provides a view into a subset of keys sharing a common prefix. |
| `subdicts()` | `dict[str, PersiDict]` | Returns a dictionary mapping all first-level key prefixes to their corresponding sub-dictionary views. |
| `discard(key)` | `bool` | Deletes a key-value pair if it exists and returns `True`; otherwise, returns `False`. |
| `get_params()` | `dict` | Returns a dictionary of the instance's configuration parameters, supporting the `mixinforge` API. |

### Conditional Operations (ETag-based)

PersiDict exposes explicit conditional operations for optimistic concurrency.
Each key has an ETag; missing keys use `ITEM_NOT_AVAILABLE`. Conditions are
`ANY_ETAG` (unconditional), `ETAG_IS_THE_SAME` (expected == actual), and
`ETAG_HAS_CHANGED` (expected != actual). Methods return a structured result
with whether the condition was satisfied, the actual ETag, the resulting ETag,
and the resulting value (or `VALUE_NOT_RETRIEVED` when value retrieval is
skipped).

Common methods and flags:

| Item | Kind | Notes |
| :--- | :--- | :--- |
| `get_item_if(key, expected_etag, condition, *, always_retrieve_value=True)` | Method | Conditional read. |
| `set_item_if(key, value, expected_etag, condition, *, always_retrieve_value=True)` | Method | Supports `KEEP_CURRENT` and `DELETE_CURRENT`. |
| `setdefault_if(key, default_value, expected_etag, condition, *, always_retrieve_value=True)` | Method | Insert-if-absent. |
| `discard_item_if(key, expected_etag, condition)` | Method | Conditional delete. |
| `transform_item(key, transformer, *, n_retries=6)` | Method | Retry loop for read-modify-write. |
| `ETagValue` | Type | NewType over `str`. |
| `ITEM_NOT_AVAILABLE` | Sentinel | Missing key marker. |
| `VALUE_NOT_RETRIEVED` | Sentinel | Value exists but was not fetched. |

Example: compare-and-swap loop

```python
from persidict import FileDirDict, ANY_ETAG, ETAG_IS_THE_SAME, ITEM_NOT_AVAILABLE

d = FileDirDict(base_dir="./data")

while True:
    r = d.get_item_if("count", ITEM_NOT_AVAILABLE, ANY_ETAG)
    new_value = 1 if r.new_value is ITEM_NOT_AVAILABLE else r.new_value + 1
    r2 = d.set_item_if("count", new_value, r.actual_etag, ETAG_IS_THE_SAME)
    if r2.condition_was_satisfied:
        break
```

## Installation

The source code is hosted on GitHub at:
[https://github.com/pythagoras-dev/persidict](https://github.com/pythagoras-dev/persidict) 

Binary installers for the latest released version are available at the Python package index at:
[https://pypi.org/project/persidict](https://pypi.org/project/persidict)

You can install `persidict` using `pip` or your favorite package manager:

```bash
pip install persidict
```

To include the AWS S3 extra dependencies:

```bash
pip install persidict[aws]
```

For development, including test dependencies:

```bash
pip install persidict[dev]
```

## Project Statistics

<!-- MIXINFORGE_STATS_START -->
| Metric | Main code | Unit Tests | Total |
|--------|-----------|------------|-------|
| Lines Of Code (LOC) | 6667 | 10319 | 16986 |
| Source Lines Of Code (SLOC) | 2956 | 6591 | 9547 |
| Classes | 28 | 8 | 36 |
| Functions / Methods | 278 | 582 | 860 |
| Files | 16 | 103 | 119 |
<!-- MIXINFORGE_STATS_END -->

## Contributing
Contributions are welcome! Please see the contributing [guide](https://github.com/pythagoras-dev/persidict/blob/master/CONTRIBUTING.md) for more details
on how to get started, run tests, and submit pull requests.

For guidance on code quality, refer to:
* [Type hints guidelines](https://github.com/pythagoras-dev/persidict/blob/master/type_hints.md)
* [Unit testing guide](https://github.com/pythagoras-dev/persidict/blob/master/unit_tests.md)

## License
`persidict` is licensed under the MIT License. See the [LICENSE](https://github.com/pythagoras-dev/persidict/blob/master/LICENSE) file for more details.

## Key Contacts

* [Vlad (Volodymyr) Pavlov](https://www.linkedin.com/in/vlpavlov/)
