from __future__ import annotations

import time
from typing import Any, Optional, Iterable

import parameterizable

from .persi_dict import PersiDict, NonEmptyPersiDictKey
from .safe_str_tuple import SafeStrTuple, NonEmptySafeStrTuple
from .singletons import EXECUTION_IS_COMPLETE


class _RAMBackend:
    """In-memory hierarchical storage backing LocalDict.

    This lightweight backend models a directory-like tree entirely in RAM and
    is used by LocalDict to provide a PersiDict-compliant interface without any
    disk or network I/O. Keys are sequences of safe strings. Each path segment
    maps to a child RAMBackend node, while leaf entries are stored in a values
    bucket per file_type.

    Attributes:
        subdicts (dict[str, _RAMBackend]):
            Mapping of first-level key segment to a child RAMBackend representing
            the corresponding subtree.
        values (dict[str, dict[str, tuple[Any, float]]]):
            Mapping of file_type to a dictionary of leaf-name -> (value, timestamp)
            pairs. The timestamp is a POSIX float seconds value (time.time()).

    Notes:
        - This backend is intentionally minimal and does not enforce character
          safety of key segments or file_type; that validation is handled by
          higher-level classes (e.g., PersiDict/LocalDict).
        - Not thread-safe or process-safe. If used concurrently, external
          synchronization is required.
        - Memory-only: all data is lost when the object is discarded.
    """

    def __init__(self):
        """Initialize an empty in-memory tree.

        Creates empty containers for child subtrees and for value buckets
        grouped by file_type. No arguments; the backend starts empty.

        Attributes initialized:
            subdicts: Empty mapping for first-level child nodes.
            values: Empty mapping for per-file_type value buckets.
        """
        self.subdicts: dict[str, _RAMBackend] = {}
        self.values: dict[str, dict[str, tuple[Any, float]]] = {}

    def child(self, name: str) -> "_RAMBackend":
        """Return a child node for the given path segment, creating if missing.

        Args:
            name (str): A single safe string segment representing the first-level
                part of a hierarchical key.

        Returns:
            _RAMBackend: The existing or newly created child backend for the
            provided segment.

        Notes:
            - This method mutates the structure by creating a child node when
              it does not exist yet.
        """
        child_backend = self.subdicts.get(name)
        if child_backend is None:
            child_backend = _RAMBackend()
            self.subdicts[name] = child_backend
        return child_backend

    def get_values_bucket(self, file_type: str) -> dict[str, tuple[Any, float]]:
        """Return the per-file_type bucket for leaf values, creating if absent.

        The bucket maps a leaf key (final segment string) to a tuple of
        (value, timestamp). The timestamp is the POSIX time when the value was
        last written.

        Args:
            file_type (str): Object type label under which values are
                grouped (e.g., "pkl", "json"). No validation is performed here.

        Returns:
            dict[str, tuple[Any, float]]: The mutable mapping for this file_type.
            Modifications affect the backend state directly.
        """
        bucket = self.values.get(file_type)
        if bucket is None:
            bucket = {}
            self.values[file_type] = bucket
        return bucket


class LocalDict(PersiDict):
    """In-memory PersiDict backed by a RAM-only hierarchical store.

    LocalDict mirrors FileDirDict semantics but keeps all data in process
    memory using a simple tree structure (RAMBackend). It is useful for tests
    and ephemeral workloads where durability is not required. Keys are
    hierarchical sequences of safe strings (SafeStrTuple). Values are stored
    per file_type and tracked with modification timestamps, providing the same
    API surface as other PersiDict implementations.

    Attributes:
        immutable_items (bool): If True, items are write-once and cannot be
            modified or deleted after initial creation.
        base_class_for_values (type | None): Optional base class that all
            stored values must inherit from. If None, any type is accepted (with
            file_type restrictions enforced by the base class).
        file_type (str): Logical serialization/format label (e.g., "pkl",
            "json") used as a namespace for values and timestamps within the
            backend.
        _backend (_RAMBackend): The in-memory tree that actually stores data.

    Notes:
        - Not thread-safe or process-safe; use external synchronization if
          accessed concurrently.
        - Memory-only: all data is lost when the object is garbage-collected or
          the process exits.
    """

    def __init__(self,
                 backend: Optional[_RAMBackend] = None,
                 file_type: str = "pkl",
                 immutable_items: bool = False,
                 base_class_for_values: Optional[type] = None, *args, **kwargs):
        """Initialize an in-memory persistent dictionary.

        Args:
            backend (_RAMBackend | None): Optional existing RAMBackend tree to
                use. If None, a new empty backend is created.
            file_type (str): Logical serialization/format label under which
                values are grouped (e.g., "pkl", "json"). Defaults to "pkl".
            immutable_items (bool): If True, items become write-once and cannot
                be modified or deleted after the first write. Defaults to False.
            base_class_for_values (type | None): Optional base class that all
                stored values must inherit from. If None, any type is accepted
                (subject to file_type restrictions). Defaults to None.

        Raises:
            ValueError: Propagated from PersiDict if file_type is empty, has
                unsafe characters, or is incompatible with value type policy.
            TypeError: Propagated from PersiDict if base_class_for_values has an
                invalid type.
        """
        self._backend = backend or _RAMBackend()
        PersiDict.__init__(self,
                           immutable_items=immutable_items,
                           base_class_for_values=base_class_for_values,
                           file_type=file_type)

    def get_params(self):
        """Return constructor parameters needed to recreate this instance.

        Note that the backend object itself is included as a reference; copying
        or reconstructing a LocalDict with this parameter will share the same
        in-memory store.

        Returns:
            dict: A dictionary of parameters (sorted by key) suitable for
            passing to the constructor.
        """
        params = dict(
            backend=self._backend,
            immutable_items=self.immutable_items,
            base_class_for_values=self.base_class_for_values,
            file_type=self.file_type,
        )
        # PersiDict.get_params sorts keys; we can reuse it by temporarily
        # creating the dict in the same form and letting parent handle sort.
        # But parent doesn't know about backend. We'll sort locally.
        return dict(sorted(params.items(), key=lambda kv: kv[0]))

    # No base_url/base_dir override: keep defaults (None)

    def __len__(self) -> int:
        """Return the total number of items stored for this file_type.

        Counts all keys across the entire in-memory tree that belong to the
        current file_type namespace.

        Returns:
            int: Total number of items.
        """
        def count(node: _RAMBackend) -> int:
            total = len(node.values.get(self.file_type, {}))
            for child in node.subdicts.values():
                total += count(child)
            return total
        return count(self._backend)

    def clear(self) -> None:
        """Remove all items under this file_type across the entire tree.

        Only entries stored for the current file_type are removed; data for
        other file types remains intact.
        """
        # Override for efficiency (optional). Remove only our file_type data.
        def clear_ft(node: _RAMBackend):
            node.values.pop(self.file_type, None)
            for ch in node.subdicts.values():
                clear_ft(ch)
        clear_ft(self._backend)

    def _navigate_to_parent(self, key: SafeStrTuple) -> tuple[_RAMBackend, str]:
        """Resolve a hierarchical key to its parent node and leaf name.

        This helper walks all segments of the key except the last one to find
        (or create) the corresponding RAMBackend node that contains the leaf
        bucket for this file_type.

        Args:
            key (SafeStrTuple): Full hierarchical key. Must be non-empty; the
                last segment is treated as the leaf item name.

        Returns:
            tuple[_RAMBackend, str]: A pair consisting of the backend node that
            holds the leaf bucket and the leaf segment (final component).
        """
        backend_node = self._backend
        for segment in key[:-1]:
            backend_node = backend_node.child(segment)
        return backend_node, key[-1]

    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Return True if the key exists in the current file_type namespace.

        Args:
            key (NonEmptyPersiDictKey): Key (string/sequence or SafeStrTuple).

        Returns:
            bool: True if the key is present; False otherwise.
        """
        key = NonEmptySafeStrTuple(key)
        parent_node, leaf = self._navigate_to_parent(key)
        bucket = parent_node.values.get(self.file_type, {})
        return leaf in bucket

    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        """Retrieve the value stored for a key.

        Args:
            key (NonEmptyPersiDictKey): Key (string/sequence or SafeStrTuple).

        Returns:
            Any: The stored value.

        Raises:
            KeyError: If the key does not exist.
            TypeError: If base_class_for_values is set and the stored value does
                not match it.
        """
        key = NonEmptySafeStrTuple(key)
        parent_node, leaf = self._navigate_to_parent(key)
        bucket = parent_node.values.get(self.file_type, {})
        if leaf not in bucket:
            raise KeyError(f"Key {tuple(key)} does not exist")
        value = bucket[leaf][0]
        if self.base_class_for_values is not None:
            if not isinstance(value, self.base_class_for_values):
                raise TypeError(
                    f"Value must be of type {self.base_class_for_values},"
                    f" but it is {type(value)} instead.")
        return value

    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any):
        """Store a value for a key.

        Interprets joker values (KEEP_CURRENT, DELETE_CURRENT) using the base
        class helper and enforces optional type restrictions if
        base_class_for_values is set.

        Args:
            key (NonEmptyPersiDictKey): Key (string/sequence or SafeStrTuple).
            value (Any): Value to store, or a joker.

        Raises:
            KeyError: If attempting to modify an existing item when
                immutable_items is True.
            TypeError: If value is a PersiDict or does not match
                base_class_for_values when it is set.
        """
        key = NonEmptySafeStrTuple(key)
        if self._process_setitem_args(key, value) is EXECUTION_IS_COMPLETE:
            return None
        parent_node, leaf = self._navigate_to_parent(key)
        bucket = parent_node.get_values_bucket(self.file_type)
        bucket[leaf] = (value, time.time())

    def __delitem__(self, key: NonEmptyPersiDictKey) -> None:
        """Delete a stored value for a key.

        Args:
            key (NonEmptyPersiDictKey): Key (string/sequence or SafeStrTuple).

        Raises:
            KeyError: If immutable_items is True or the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        self._process_delitem_args(key)
        parent_node, leaf = self._navigate_to_parent(key)
        bucket = parent_node.values.get(self.file_type, {})
        if leaf not in bucket:
            raise KeyError(f"Key {tuple(key)} does not exist")
        del bucket[leaf]

    def _generic_iter(self, result_type: set[str]):
        """Underlying implementation for keys/values/items/timestamps iterators.

        Traverses the in-memory tree and yields entries based on the requested
        result_type. The shapes of yielded items mirror FileDirDict:
          - {"keys"} -> SafeStrTuple
          - {"values"} -> Any
          - {"keys", "values"} -> tuple[SafeStrTuple, Any]
          - {"keys", "values", "timestamps"} or {"keys", "timestamps"}
            -> tuples that end with a float POSIX timestamp.

        Args:
            result_type (set[str]): Any non-empty subset of {"keys", "values",
                "timestamps"} specifying which fields to yield.

        Returns:
            Iterator: A generator over requested items.

        Raises:
            TypeError: If result_type is not a set.
            ValueError: If result_type is empty or contains unsupported labels.
        """
        self._process_generic_iter_args(result_type)

        def walk(prefix: tuple[str, ...], node: _RAMBackend):
            # yield values at this level
            bucket = node.values.get(self.file_type, {})
            for leaf, (val, ts) in bucket.items():
                full_key = SafeStrTuple((*prefix, leaf))
                to_return: list[Any] = []
                if "keys" in result_type:
                    to_return.append(full_key)
                if "values" in result_type:
                    to_return.append(val)
                if len(result_type) == 1:
                    yield to_return[0]
                else:
                    if "timestamps" in result_type:
                        to_return.append(ts)
                    yield tuple(to_return)
            # then recurse into children
            for name, child in node.subdicts.items():
                yield from walk((*prefix, name), child)

        return walk((), self._backend)

    def timestamp(self, key: NonEmptyPersiDictKey) -> float:
        """Return the last modification time of a key.

        Args:
            key (NonEmptyPersiDictKey): Key (string/sequence or SafeStrTuple).

        Returns:
            float: POSIX timestamp (seconds since Unix epoch) when the value was
                last written.

        Raises:
            KeyError: If the key does not exist.
        """
        key = NonEmptySafeStrTuple(key)
        parent_node, leaf = self._navigate_to_parent(key)
        bucket = parent_node.values.get(self.file_type, {})
        if leaf not in bucket:
            raise KeyError(f"Key {tuple(key)} does not exist")
        return bucket[leaf][1]

    def get_subdict(self, prefix_key: Iterable[str] | SafeStrTuple) -> PersiDict:
        """Return a view rooted at the given key prefix.

        The returned LocalDict shares the same underlying RAMBackend, but its
        root is moved to the subtree identified by prefix_key. If intermediate
        nodes do not exist, they are created (resulting in an empty subdict).

        Args:
            prefix_key (Iterable[str] | SafeStrTuple): Key prefix identifying the
                subtree to expose. May be empty to refer to the current root.

        Returns:
            PersiDict: A LocalDict instance whose operations are restricted to
                the keys under the specified prefix.
        """
        prefix = SafeStrTuple(prefix_key) if not isinstance(prefix_key, SafeStrTuple) else prefix_key
        root_node = self._backend
        for segment in prefix:
            root_node = root_node.child(segment)
        # Create a new LocalDict rooted at this backend
        return LocalDict(backend=root_node,
                         file_type=self.file_type,
                         immutable_items=self.immutable_items,
                         base_class_for_values=self.base_class_for_values)


# parameterizable.register_parameterizable_class(LocalDict)