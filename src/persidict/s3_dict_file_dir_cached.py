"""S3Dict_FileDirCached implementation that mimics S3Dict_Legacy but uses BasicS3Dict, FileDirDict, and cached classes."""

from __future__ import annotations

from typing import Any, Optional

import parameterizable
from parameterizable import sort_dict_by_keys

from .basic_s3_dict import BasicS3Dict
from .file_dir_dict import FileDirDict, FILEDIRDICT_DEFAULT_BASE_DIR
from .cached_appendonly_dict import AppendOnlyDictCached
from .cached_mutable_dict import MutableDictCached
from .persi_dict import PersiDict, NonEmptyPersiDictKey, PersiDictKey
from .safe_str_tuple import NonEmptySafeStrTuple
from .overlapping_multi_dict import OverlappingMultiDict


# Default base directory for S3Dict_FileDirCached local cache
S3DICT_NEW_DEFAULT_BASE_DIR = "__s3_dict__"


class S3Dict_FileDirCached(PersiDict):
    """S3-backed persistent dictionary using BasicS3Dict with local caching.
    
    This class mimics the interface and behavior of S3Dict_Legacy but internally uses
    BasicS3Dict for S3 operations combined with FileDirDict-based local caching
    via the cached wrapper classes (AppendOnlyDictCached/MutableDictCached).
    
    The architecture layers caching on top of BasicS3Dict to provide:
    - Fast local access for frequently accessed items
    - Efficient batch operations
    - ETag-based change detection for mutable dictionaries
    - Optimized append-only performance when append_only=True
    """
    
    def __init__(self, bucket_name: str = "my_bucket",
                 region: str = None,
                 root_prefix: str = "",
                 base_dir: str = S3DICT_NEW_DEFAULT_BASE_DIR,
                 serialization_format: str = "pkl",
                 digest_len: int = 8,
                 append_only: bool = False,
                 base_class_for_values: Optional[type] = None,
                 *args, **kwargs):
        """Initialize an S3-backed persistent dictionary with local caching.

        Args:
            bucket_name: Name of the S3 bucket to use.
            region: AWS region for the bucket.
            root_prefix: Common S3 key prefix under which all objects are stored.
            base_dir: Local directory path for caching.
            serialization_format: File extension/format for stored values.
            digest_len: Number of base32 MD5 hash characters for collision prevention.
            append_only: If True, prevents modification/deletion of existing items.
            base_class_for_values: Optional base class that all stored values must inherit from.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(append_only=append_only,
                         base_class_for_values=base_class_for_values,
                         serialization_format=serialization_format)
        
        # Create the main S3 storage using BasicS3Dict
        self._main_dict = BasicS3Dict(
            bucket_name=bucket_name,
            region=region,
            root_prefix=root_prefix,
            serialization_format=serialization_format,
            append_only=append_only,
            base_class_for_values=base_class_for_values
        )
        
        # Set up local cache parameters for FileDirDict
        individual_subdicts_params = {self.serialization_format: {}}
        
        if not append_only:
            self.etag_serialization_format = f"{self.serialization_format}_etag"
            individual_subdicts_params[self.etag_serialization_format] = {
                "base_class_for_values": str}
        
        # Create local cache using OverlappingMultiDict with FileDirDict
        self.local_cache = OverlappingMultiDict(
            dict_type=FileDirDict,
            shared_subdicts_params={
                "base_dir": base_dir,
                "append_only": append_only,
                "base_class_for_values": base_class_for_values,
                "digest_len": digest_len
            },
            **individual_subdicts_params)
        
        # Get the data cache
        self._data_cache = getattr(self.local_cache, self.serialization_format)
        
        # Create the appropriate cached wrapper
        if append_only:
            # Use AppendOnlyDictCached for append-only mode
            self._cached_dict = AppendOnlyDictCached(
                main_dict=self._main_dict,
                data_cache=self._data_cache
            )
        else:
            # Use MutableDictCached for mutable mode with ETag cache
            self._etag_cache = getattr(self.local_cache, self.etag_serialization_format)
            self._cached_dict = MutableDictCached(
                main_dict=self._main_dict,
                data_cache=self._data_cache,
                etag_cache=self._etag_cache
            )
    
    @property
    def digest_len(self) -> int:
        """Get the digest length used for collision prevention."""
        return self._data_cache.digest_len
    
    def get_params(self):
        """Return configuration parameters as a dictionary."""
        # Get params from the main dict and local cache
        params = self._main_dict.get_params()
        cache_params = self._data_cache.get_params()
        
        # Add cache-specific params
        params["base_dir"] = cache_params["base_dir"]
        params["digest_len"] = cache_params["digest_len"]

        params = sort_dict_by_keys(params)
        
        return params
    
    @property
    def base_url(self) -> str:
        """Get the base S3 URL."""
        return self._main_dict.base_url
    
    @property
    def base_dir(self) -> str:
        """Get the base directory for local cache."""
        return self._data_cache.base_dir
    
    def __contains__(self, key: NonEmptyPersiDictKey) -> bool:
        """Check if key exists in the dictionary."""
        return self._cached_dict.__contains__(key)
    
    def __getitem__(self, key: NonEmptyPersiDictKey) -> Any:
        """Get item from dictionary."""
        return self._cached_dict.__getitem__(key)
    
    def __setitem__(self, key: NonEmptyPersiDictKey, value: Any) -> None:
        """Set item in dictionary."""
        self._cached_dict.__setitem__(key, value)
    
    def __delitem__(self, key: NonEmptyPersiDictKey) -> None:
        """Delete item from dictionary."""
        self._cached_dict.__delitem__(key)
    
    def __len__(self) -> int:
        """Get number of items in dictionary."""
        return self._cached_dict.__len__()
    
    def _generic_iter(self, result_type: set[str]):
        """Generic iteration over dictionary items."""
        return self._cached_dict._generic_iter(result_type)
    
    def get_subdict(self, key: PersiDictKey):
        """Get a subdictionary for the given key prefix."""
        return self._main_dict.get_subdict(key)
    
    def timestamp(self, key: NonEmptyPersiDictKey):
        """Get the timestamp of when the item was last modified."""
        return self._cached_dict.timestamp(key)
    
    # Additional methods that might be needed for ETag support
    def get_item_if_etag_changed(self, key: NonEmptyPersiDictKey, etag: Optional[str]):
        """Get item only if ETag has changed (for mutable dicts)."""
        if hasattr(self._cached_dict, 'get_item_if_etag_changed'):
            return self._cached_dict.get_item_if_etag_changed(key, etag)
        else:
            # For append-only dicts, just get the item
            return self._cached_dict.__getitem__(key)
    
    def set_item_get_etag(self, key: NonEmptyPersiDictKey, value: Any):
        """Set item and return ETag (for mutable dicts)."""
        if hasattr(self._cached_dict, 'set_item_get_etag'):
            return self._cached_dict.set_item_get_etag(key, value)
        else:
            # For append-only dicts, just set the item
            self._cached_dict.__setitem__(key, value)
            return None
    
    def discard(self, key: NonEmptyPersiDictKey) -> bool:
        """Delete an item without raising an exception if it doesn't exist.
        
        This method fixes the issue where cached dictionaries return multiple
        success counts for a single key deletion.
        
        Args:
            key: Key to delete.
            
        Returns:
            bool: True if the item existed and was deleted; False otherwise.
        """
        key = NonEmptySafeStrTuple(key)

        try:
            del self[key]
            return True
        except KeyError:
            return False


S3Dict = S3Dict_FileDirCached # Alias for backward compatibility


# parameterizable.register_parameterizable_class(S3Dict_FileDirCached)