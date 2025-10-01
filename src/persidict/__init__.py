"""Persistent dictionaries that store key-value pairs on local disks or AWS S3.

This package provides a unified interface for persistent dictionary-like
storage with various backends including filesystem and AWS S3.

Classes:
    PersiDict: Abstract base class defining the unified interface for all
        persistent dictionaries.
    NonEmptySafeStrTuple: A flat tuple of URL/filename-safe strings that
        can be used as a key for PersiDict objects.
    FileDirDict: A dictionary that stores key-value pairs as files on a
        local hard drive. Keys compose filenames, values are stored as
        pickle or JSON objects.
    S3Dict_Legacy: A dictionary that stores key-value pairs as S3 objects on AWS.
        Keys compose object names, values are stored as pickle or JSON S3 objects.
    BasicS3Dict: A basic S3-backed dictionary with direct S3 operations.
    WriteOnceDict: A write-once wrapper that prevents modification of existing
        items after initial storage.
    EmptyDict: Equivalent of null device in OS - accepts all writes but discards
        them, returns nothing on reads. Always appears empty regardless of
        operations performed. Useful for testing, debugging, or as a placeholder.
    OverlappingMultiDict: A dictionary that can handle overlapping key spaces.

Functions:
    get_safe_chars(): Returns a set of URL/filename-safe characters permitted
        in keys.
    replace_unsafe_chars(): Replaces forbidden characters in a string with
        safe alternatives.

Constants:
    KEEP_CURRENT, DELETE_CURRENT: Special joker values for conditional operations.

Note:
    All persistent dictionaries support multiple serialization formats, including
    pickle and JSON, with automatic type handling and collision-safe key encoding.
"""
from .safe_chars import *
from .safe_str_tuple import *
from .persi_dict import PersiDict, PersiDictKey
from .file_dir_dict import FileDirDict
from .s3_dict_file_dir_cached import S3Dict_FileDirCached, S3Dict
from .basic_s3_dict import BasicS3Dict
from .write_once_dict import WriteOnceDict
from .empty_dict import EmptyDict
from .singletons import Joker, KeepCurrentFlag, DeleteCurrentFlag
from .singletons import KEEP_CURRENT, DELETE_CURRENT
from .overlapping_multi_dict import OverlappingMultiDict
from .cached_appendonly_dict import AppendOnlyDictCached
from .cached_mutable_dict import MutableDictCached
from .local_dict import LocalDict