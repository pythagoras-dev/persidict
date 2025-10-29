persidict documentation
=======================

.. image:: https://img.shields.io/pypi/v/persidict.svg
   :target: https://pypi.org/project/persidict/
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/persidict.svg
   :target: https://github.com/pythagoras-dev/persidict
   :alt: Python versions

.. image:: https://img.shields.io/badge/License-MIT-blue.svg
   :target: https://github.com/pythagoras-dev/persidict/blob/master/LICENSE
   :alt: License: MIT

.. image:: https://img.shields.io/pypi/dm/persidict?color=blue
   :target: https://pypistats.org/packages/persidict
   :alt: Downloads

.. image:: https://app.readthedocs.org/projects/persidict/badge/?version=latest
   :target: https://persidict.readthedocs.io/en/latest/
   :alt: Documentation Status

.. image:: https://img.shields.io/badge/code_style-pep8-blue.svg
   :target: https://peps.python.org/pep-0008/
   :alt: Code style: pep8

.. image:: https://img.shields.io/badge/docstrings_style-Google-blue
   :target: https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
   :alt: Docstring Style: Google


**persidict** is a lightweight persistent key-value store for Python designed for distributed
environments where multiple processes on different machines concurrently work with the same store.

Overview
--------

``persidict`` provides a familiar dict-like API for persistent storage, supporting both local
filesystem and AWS S3 backends. Each key-value pair is stored as a separate file or S3 object,
enabling efficient concurrent access in distributed computing scenarios.

.. code-block:: python

   from persidict import FileDirDict, S3Dict

   # Local storage
   local_store = FileDirDict(base_dir="my_data")
   local_store["key"] = "value"

   # Cloud storage
   cloud_store = S3Dict(bucket_name="my-bucket")
   cloud_store["api_key"] = "ABC-123"

Key Features
------------

* **Persistent Storage**: Store dictionaries on local filesystem (``FileDirDict``) or AWS S3 (``S3Dict``)
* **Standard Dictionary API**: Use like regular Python dicts with ``[]``, ``keys()``, ``items()``, etc.
* **Distributed-Ready**: Optimistic concurrency model designed for multi-process, multi-machine access
* **Flexible Serialization**: Support for pickle, JSON, or plain text storage formats
* **Type Safety**: Optional enforcement that all values are instances of a specific class
* **Advanced Features**: Write-once dictionaries, timestamps, ETags, hierarchical keys, caching layers
* **Hierarchical Keys**: Keys can be sequences of strings, creating directory-like structures

Core Concepts
-------------

PersiDict Base Class
^^^^^^^^^^^^^^^^^^^^

``PersiDict`` is the abstract base class defining the unified interface for all persistent
dictionaries. It extends Python's ``MutableMapping`` with persistence-specific operations.

Key Types
^^^^^^^^^

Keys in persidict must be URL/filename-safe:

* **SafeStrTuple**: An immutable tuple of non-empty, filesystem-safe strings
* **NonEmptySafeStrTuple**: A non-empty SafeStrTuple (the standard key type)
* Keys can be provided as strings or sequences of strings and are automatically converted

Storage Implementations
^^^^^^^^^^^^^^^^^^^^^^^

**FileDirDict**
   Local filesystem storage. Each key-value pair is a separate file in a directory hierarchy.

   .. code-block:: python

      from persidict import FileDirDict

      store = FileDirDict(
          base_dir="./data",
          serialization_format="json",  # or "pkl" or "txt"
          append_only=False,
          digest_len=8  # hash suffix for case-insensitive filesystems
      )

**S3Dict**
   AWS S3 cloud storage. Each key-value pair is an S3 object.

   .. code-block:: python

      from persidict import S3Dict

      store = S3Dict(
          bucket_name="my-bucket",
          region="us-east-1",
          serialization_format="pkl"
      )

**LocalDict**
   In-memory storage for testing and ephemeral data.

   .. code-block:: python

      from persidict import LocalDict

      store = LocalDict()

**EmptyDict**
   Null device equivalent - accepts all writes but discards them. Always appears empty.
   Useful for testing and debugging.

   .. code-block:: python

      from persidict import EmptyDict

      store = EmptyDict()  # All operations work but nothing is stored

Advanced Wrappers
^^^^^^^^^^^^^^^^^

**WriteOnceDict**
   Enforces write-once behavior with optional probabilistic consistency checking.

   .. code-block:: python

      from persidict import WriteOnceDict, FileDirDict

      store = WriteOnceDict(
          wrapped_dict=FileDirDict(append_only=True),
          p_consistency_checks=0.1  # 10% random validation
      )

**MutableDictCached**
   Adds intelligent caching with ETag validation for mutable dictionaries.

   .. code-block:: python

      from persidict import MutableDictCached, FileDirDict, LocalDict

      store = MutableDictCached(
          main_dict=FileDirDict(base_dir="./data"),
          data_cache=LocalDict(),
          etag_cache=LocalDict()
      )

**AppendOnlyDictCached**
   High-performance caching for immutable (append-only) dictionaries.

   .. code-block:: python

      from persidict import AppendOnlyDictCached, FileDirDict, LocalDict

      store = AppendOnlyDictCached(
          main_dict=FileDirDict(base_dir="./data", append_only=True),
          data_cache=LocalDict()
      )

**OverlappingMultiDict**
   Container for multiple PersiDict instances with different serialization formats
   sharing the same storage location.

   .. code-block:: python

      from persidict import OverlappingMultiDict, FileDirDict

      multi = OverlappingMultiDict(
          dict_type=FileDirDict,
          shared_subdicts_params={"base_dir": "./data"},
          json={},  # Creates multi.json with serialization_format="json"
          pkl={},   # Creates multi.pkl with serialization_format="pkl"
          txt={}    # Creates multi.txt with serialization_format="txt"
      )

      multi.json["config"] = {"setting": "value"}
      multi.pkl["model"] = trained_model
      multi.txt["log"] = "Plain text log entry"

Configuration Parameters
------------------------

Common Parameters
^^^^^^^^^^^^^^^^^

All PersiDict implementations support these parameters:

``serialization_format`` : str, default="pkl"
    Storage format: ``"pkl"`` (pickle), ``"json"`` (JSON), or any other value for plain text

``base_class_for_values`` : type | None, default=None
    Optional type constraint - all values must be instances of this class

``append_only`` : bool, default=False
    If True, items cannot be modified or deleted after creation

FileDirDict Specific
^^^^^^^^^^^^^^^^^^^^

``base_dir`` : str
    Directory path for storing files

``digest_len`` : int, default=0
    Length of hash suffix added to prevent collisions on case-insensitive filesystems

S3Dict Specific
^^^^^^^^^^^^^^^

``bucket_name`` : str
    Name of the S3 bucket

``region`` : str | None, default=None
    AWS region for the bucket (uses default region if not specified)

``base_dir`` : str
    Local directory for caching downloaded files

Special Values (Jokers)
-----------------------

persidict provides special command-like values for conditional operations:

``KEEP_CURRENT``
   When assigned to a key, preserves the existing value unchanged

   .. code-block:: python

      from persidict import FileDirDict, KEEP_CURRENT

      store = FileDirDict(base_dir="./data")
      store["key"] = "original"
      store["key"] = KEEP_CURRENT  # Value remains "original"

``DELETE_CURRENT``
   When assigned to a key, deletes that key-value pair

   .. code-block:: python

      from persidict import FileDirDict, DELETE_CURRENT

      store = FileDirDict(base_dir="./data")
      store["key"] = "value"
      store["key"] = DELETE_CURRENT  # Key is now deleted

Extended API Methods
--------------------

Beyond standard dict operations, PersiDict provides additional methods:

Timestamp Operations
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   timestamp(key) -> float
       Returns POSIX timestamp of key's last modification

   oldest_keys(max_n=None) -> list[SafeStrTuple]
       Returns keys sorted from oldest to newest

   newest_keys(max_n=None) -> list[SafeStrTuple]
       Returns keys sorted from newest to oldest

   oldest_values(max_n=None) -> list[Any]
       Returns values corresponding to oldest keys

   newest_values(max_n=None) -> list[Any]
       Returns values corresponding to newest keys

Hierarchical Operations
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   get_subdict(prefix_key) -> PersiDict
       Returns a view into keys sharing a common prefix

   subdicts() -> dict[str, PersiDict]
       Returns mapping of first-level key prefixes to sub-dictionaries

Utility Methods
^^^^^^^^^^^^^^^

.. code-block:: python

   random_key() -> SafeStrTuple | None
       Returns a uniformly random key (None if empty)

   discard(key) -> bool
       Deletes key if it exists, returns True; otherwise returns False

   get_params() -> dict
       Returns configuration parameters (parameterizable API)

ETag Operations
^^^^^^^^^^^^^^^

.. code-block:: python

   etag(key) -> str | None
       Returns ETag for the key (or timestamp-based equivalent)

   set_item_get_etag(key, value) -> str | None
       Stores value and returns new ETag

   get_item_if_etag_changed(key, etag) -> tuple[Any, str|None] | ETagHasNotChangedFlag
       Retrieves value only if ETag changed

Enhanced Iterators
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   keys_and_timestamps() -> Iterator[tuple[SafeStrTuple, float]]
       Iterates over (key, timestamp) pairs

   values_and_timestamps() -> Iterator[tuple[Any, float]]
       Iterates over (value, timestamp) pairs

   items_and_timestamps() -> Iterator[tuple[SafeStrTuple, Any, float]]
       Iterates over (key, value, timestamp) triples

Design Principles
-----------------

1. **Familiar dict-like API**: Mirrors Python's built-in dict interface
2. **Optimistic concurrency**: Assumes conflicts are rare; last-write-wins for mutations
3. **Pluggable backends**: Unified API across filesystem, S3, and in-memory storage
4. **Hierarchical keys**: Sequences of safe strings form natural directory structures
5. **Flexible serialization**: Choose pickle, JSON, or plain text per use case
6. **Layered architecture**: Compose capabilities (storage + caching + write-once)
7. **Intelligent caching**: Tune performance for mutable vs append-only access patterns

Trade-offs
^^^^^^^^^^

* **Eventual consistency**: May briefly see stale data in distributed scenarios
* **No multi-key transactions**: Single-key operations are atomic only
* **Memory vs speed**: Caching trades memory for performance
* **Network dependency**: Cloud backends require reliable connectivity

Use Cases
---------

persidict excels at:

* **Caching**: Store expensive computation results accessible across machines
* **Configuration Management**: Distribute application settings in multi-node deployments
* **Data Pipelines**: Share data between pipeline stages
* **Distributed Task Queues**: Store task definitions and results
* **Memoization**: Cache function results persistently and distributedly
* **Model Registries**: Store and version machine learning models
* **Experiment Tracking**: Log and retrieve experiment parameters and results

Choosing a Configuration
-------------------------

**Development/Testing**
   ``LocalDict`` or ``FileDirDict`` for simplicity

   .. code-block:: python

      store = FileDirDict(base_dir="./dev_data")

**Production, Single Machine**
   ``MutableDictCached`` with ``FileDirDict`` for performance

   .. code-block:: python

      from persidict import MutableDictCached, FileDirDict, LocalDict

      store = MutableDictCached(
          main_dict=FileDirDict(base_dir="/var/app/data"),
          data_cache=LocalDict(),
          etag_cache=LocalDict()
      )

**Production, Distributed**
   ``MutableDictCached`` with ``S3Dict`` for scalability

   .. code-block:: python

      from persidict import MutableDictCached, S3Dict, LocalDict

      store = MutableDictCached(
          main_dict=S3Dict(bucket_name="prod-data"),
          data_cache=LocalDict(),
          etag_cache=LocalDict()
      )

**Append-Only Workloads**
   ``AppendOnlyDictCached`` for maximum performance

   .. code-block:: python

      from persidict import AppendOnlyDictCached, FileDirDict, LocalDict

      store = AppendOnlyDictCached(
          main_dict=FileDirDict(base_dir="./data", append_only=True),
          data_cache=LocalDict()
      )

**Content-Addressed Storage**
   ``WriteOnceDict`` to avoid redundant writes

   .. code-block:: python

      from persidict import WriteOnceDict, FileDirDict

      store = WriteOnceDict(
          wrapped_dict=FileDirDict(append_only=True),
          p_consistency_checks=0.05
      )

Installation
------------

Install from PyPI:

.. code-block:: bash

   pip install persidict

With AWS S3 support:

.. code-block:: bash

   pip install persidict[aws]

For development:

.. code-block:: bash

   pip install persidict[dev]

Dependencies
^^^^^^^^^^^^

Core dependencies:

* parameterizable
* jsonpickle
* joblib
* lz4
* deepdiff

AWS S3 support:

* boto3

Development/testing:

* pandas
* numpy
* pytest
* moto

Quick Start Examples
--------------------

Basic Usage
^^^^^^^^^^^

.. code-block:: python

   from persidict import FileDirDict

   # Create a persistent dictionary
   cache = FileDirDict(base_dir="./cache")

   # Use it like a regular dict
   cache["user_123"] = {"name": "Alice", "score": 95}
   cache["user_456"] = {"name": "Bob", "score": 87}

   print(cache["user_123"])  # {'name': 'Alice', 'score': 95}
   print(len(cache))  # 2
   print("user_123" in cache)  # True

   # Data persists across sessions
   cache2 = FileDirDict(base_dir="./cache")
   print(cache2["user_123"])  # Still there!

Hierarchical Keys
^^^^^^^^^^^^^^^^^

.. code-block:: python

   from persidict import FileDirDict

   store = FileDirDict(base_dir="./data")

   # Keys can be sequences of strings
   store[("users", "alice", "profile")] = {"age": 30}
   store[("users", "bob", "profile")] = {"age": 25}
   store[("configs", "database")] = {"host": "localhost"}

   # Get subdictionaries
   users = store.get_subdict("users")
   print(len(users))  # 2 (alice and bob)

   # Access nested data
   alice = store.get_subdict(("users", "alice"))
   print(alice["profile"])  # {'age': 30}

Timestamps and Sorting
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from persidict import FileDirDict
   import time

   log = FileDirDict(base_dir="./logs")

   log["event_1"] = "Started"
   time.sleep(0.1)
   log["event_2"] = "Processing"
   time.sleep(0.1)
   log["event_3"] = "Completed"

   # Get events in chronological order
   for key in log.oldest_keys():
       timestamp = log.timestamp(key)
       print(f"{key}: {log[key]} at {timestamp}")

   # Get most recent events
   recent = log.newest_keys(max_n=2)
   print(recent)  # [('event_3',), ('event_2',)]

Caching for Performance
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from persidict import MutableDictCached, FileDirDict, LocalDict

   # Slow remote storage (e.g., network drive)
   remote = FileDirDict(base_dir="/mnt/network/data")

   # Fast local caches
   data_cache = LocalDict()
   etag_cache = LocalDict()

   # Cached access
   fast_store = MutableDictCached(
       main_dict=remote,
       data_cache=data_cache,
       etag_cache=etag_cache
   )

   # First access: slow (reads from remote)
   value = fast_store["key"]

   # Subsequent accesses: fast (reads from cache)
   value = fast_store["key"]  # Much faster!

Type Safety
^^^^^^^^^^^

.. code-block:: python

   from persidict import FileDirDict
   import pandas as pd

   # Only allow pandas DataFrames
   df_store = FileDirDict(
       base_dir="./dataframes",
       base_class_for_values=pd.DataFrame,
       serialization_format="json"
   )

   df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
   df_store["data"] = df  # OK

   # df_store["text"] = "hello"  # Raises TypeError!

Multiple Serialization Formats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from persidict import OverlappingMultiDict, FileDirDict

   multi = OverlappingMultiDict(
       dict_type=FileDirDict,
       shared_subdicts_params={"base_dir": "./storage"},
       json={},
       pkl={},
       txt={}
   )

   # Store same logical data in different formats
   config = {"host": "localhost", "port": 8080}
   multi.json["config"] = config  # Human-readable JSON
   multi.pkl["config"] = config   # Efficient pickle

   # Store plain text
   multi.txt["readme"] = "This is a plain text file."

API Reference
-------------

.. toctree::
   :maxdepth: 2
   :caption: API Documentation:

   api/modules

Contributing
------------

Contributions are welcome! Please see the `contributing guide <https://github.com/pythagoras-dev/persidict/blob/master/contributing.md>`_
for details on:

* Setting up the development environment
* Running tests
* Code style guidelines
* Commit message conventions
* Submitting pull requests

License
-------

persidict is licensed under the MIT License.
See the `LICENSE <https://github.com/pythagoras-dev/persidict/blob/master/LICENSE>`_ file for details.

Resources
---------

* **GitHub**: https://github.com/pythagoras-dev/persidict
* **PyPI**: https://pypi.org/project/persidict/
* **Documentation**: https://persidict.readthedocs.io/
* **Design Principles**: `design_principles.md <https://github.com/pythagoras-dev/persidict/blob/master/design_principles.md>`_

Contact
-------

* **Maintainer**: `Vlad (Volodymyr) Pavlov <https://www.linkedin.com/in/vlpavlov/>`_
* **Email**: vlpavlov@ieee.org

Indices and Tables
==================

* :ref:`genindex`
* :ref:`search`