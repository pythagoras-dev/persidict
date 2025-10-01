# persidict

Simple persistent dictionaries for distributed applications in Python.

## 1. What Is It?

`persidict` is a lightweight persistent key-value store for Python. 
It saves a dictionary to either a local directory or an AWS S3 bucket, 
storing each value as its own file or S3 object. Keys are limited to 
text strings or sequences of strings.

In contrast to traditional persistent dictionaries (e.g., Python’s `shelve)`, 
`persidict` is [designed](https://github.com/pythagoras-dev/persidict/blob/master/design_principles.md) 
for distributed environments where multiple processes 
on different machines concurrently work with the same store.

## 2. Why Use It?

## 2.1 Features

* **Persistent Storage**: Save dictionaries to the local filesystem 
(`FileDirDict`) or AWS S3 (`S3Dict`).
* **Standard Dictionary API**: Use persidict objects like standard 
Python dictionaries with methods like `__getitem__`, `__setitem__`, 
`__delitem__`, `keys`, `values`, `items`, etc.
* **Distributed Computing Ready**: Designed for concurrent access 
in distributed environments.
* **Flexible Serialization**: Store values as pickles (`pkl`), 
JSON (`json`), or plain text.
* **Type Safety**: Optionally enforce that all values in a dictionary are 
instances of a specific class.
* **Advanced Functionality**: Includes features like write-once dictionaries, 
timestamping of entries, and tools for handling file-system-safe keys.
* **Hierarchical Keys**: Keys can be sequences of strings, 
creating a directory-like structure within the storage backend.

## 2.2 Use Cases

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

## 3. Usage

### 3.1 Storing Data on a Local Disk

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
### 3.2 Storing Data in the Cloud (AWS S3)

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

## 4. Comparison With Python Built-in Dictionaries

### 4.1 Similarities 

`PersiDict` subclasses can be used like regular Python dictionaries, supporting: 

* Get, set, and delete operations with square brackets (`[]`).
* Iteration over keys, values, and items.
* Membership testing with `in`.
* Length checking with `len()`.
* Standard methods like `keys()`, `values()`, `items()`, `get()`, `clear()`
, `setdefault()`, and `update()`.

### 4.2 Differences 

* **Persistence**: Data is saved between program executions.
* **Keys**: Keys must be URL/filename-safe strings or their sequences.
* **Values**: Values must be pickleable. 
You can also constrain values to a specific class.
* **Order**: Insertion order is not preserved.
* **Additional Methods**: `PersiDict` provides extra methods not in the standard 
dict API, such as `timestamp()`, `etag()`, `random_key()`, `newest_keys()`
, `subdicts()`, `discard()`, `get_params()` and more.
* **Special Values**: Use `KEEP_CURRENT` to avoid updating a value 
and `DELETE_CURRENT` to delete a value during an assignment.

## 5. Glossary

### 5.1 Core Concepts

* **`PersiDict`**: The abstract base class that defines the common interface 
for all persistent dictionaries in the package. It's the foundation 
upon which everything else is built.
* **`NonEmptyPersiDictKey`**: A type hint that specifies what can be used
as a key in any `PersiDict`. It can be a `NonEmptySafeStrTuple`, a single string, 
or a sequence of strings. When a `PesiDict` method requires a key as an input,
it will accept any of these types and convert them to 
a `NonEmptySafeStrTuple` internally.
* **`NonEmptySafeStrTuple`**: The core data structure for keys. 
It's an immutable, flat tuple of non-empty, URL/filename-safe strings, 
ensuring that keys are consistent and safe for various storage backends. 
When a `PersiDict` method returns a key, it will always be in this format.

### 5.2 Main Implementations

* **`FileDirDict`**: A primary, concrete implementation of `PersiDict` 
that stores each key-value pair as a separate file in a local directory.
* **`S3Dict`**: The other primary implementation of `PersiDict`, 
which stores each key-value pair as an object in an AWS S3 bucket, 
suitable for distributed environments.

### 5.3 Key Parameters

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

### 5.4 Advanced and Supporting Classes

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
like a null device in OS - accepts all writes but discards them, 
returns nothing on reads. Always appears empty 
regardless of operations performed on it.

### 5.5 Special "Joker" Values

* **`Joker`**: The base class for special command-like values that 
can be assigned to a key to trigger an action instead of storing a value.
* **`KEEP_CURRENT`**: A "joker" value that, when assigned to a key, 
ensures the existing value is not changed.
* **`DELETE_CURRENT`**: A "joker" value that deletes the key-value pair 
from the dictionary when assigned to a key.

## 6. API Highlights

`PersiDict` subclasses support the standard Python dictionary API, plus these additional methods for advanced functionality:

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
| `get_params()` | `dict` | Returns a dictionary of the instance's configuration parameters, supporting the `parameterizable` API. |

## 7. Installation

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

## 8. Dependencies

`persidict` has the following core dependencies:

* [parameterizable](https://pypi.org/project/parameterizable/)
* [jsonpickle](https://jsonpickle.github.io)
* [joblib](https://joblib.readthedocs.io)
* [lz4](https://python-lz4.readthedocs.io)
* [deepdiff](https://zepworks.com/deepdiff)

For AWS S3 support (`S3Dict`), you will also need:
* [boto3](https://boto3.readthedocs.io)

For development and testing, the following packages are used:
* [pandas](https://pandas.pydata.org)
* [numpy](https://numpy.org)
* [pytest](https://pytest.org)
* [moto](http://getmoto.org)

## 9. Contributing
Contributions are welcome! Please see the contributing [guide](https://github.com/pythagoras-dev/persidict?tab=contributing-ov-file) for more details 
on how to get started, run tests, and submit pull requests.

## 10. License
`persidict` is licensed under the MIT License. See the [LICENSE](https://github.com/pythagoras-dev/persidict?tab=MIT-1-ov-file) file for more details.

## 11. Key Contacts

* [Vlad (Volodymyr) Pavlov](https://www.linkedin.com/in/vlpavlov/)