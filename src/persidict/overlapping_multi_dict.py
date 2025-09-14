from __future__ import annotations

from typing import Any, Dict, List, Type

from .persi_dict import PersiDict

class OverlappingMultiDict:
    """Container for multiple PersiDict instances sharing the same base and differing by file_type.

    This class instantiates several sub-dictionaries (PersiDict subclasses) that
    share common parameters but differ by their file_type. Each sub-dictionary is
    exposed as an attribute whose name equals the file_type (e.g., obj.json, obj.csv).
    All sub-dictionaries typically point to the same underlying base directory or
    bucket and differ only in how items are materialized by file type.

    Attributes:
        dict_type (Type[PersiDict]): A subclass of PersiDict used to create each 
            sub-dictionary.
        shared_subdicts_params (Dict[str, Any]): Parameters applied to every 
            created sub-dictionary (e.g., base_dir, bucket, immutable_items, 
            digest_len).
        individual_subdicts_params (Dict[str, Dict[str, Any]]): Mapping from 
            file_type (attribute name) to a dict of parameters that are specific 
            to that sub-dictionary. These override or extend shared_subdicts_params 
            for the given file_type.
        subdicts_names (List[str]): The list of file_type names (i.e., attribute 
            names) created.

    Raises:
        TypeError: If pickling is attempted or item access is used on the
            OverlappingMultiDict itself rather than its sub-dicts.
    """
    def __init__(self,
                 dict_type: type[PersiDict],
                 shared_subdicts_params: dict[str, Any],
                 **individual_subdicts_params: dict[str, Any]) -> None:
        """Initialize the container and create sub-dictionaries.

        Args:
            dict_type (Type[PersiDict]): A subclass of PersiDict that will be 
                instantiated for each file_type provided via individual_subdicts_params.
            shared_subdicts_params (Dict[str, Any]): Parameters shared by all 
                sub-dicts (e.g., base_dir, bucket).
            **individual_subdicts_params (Dict[str, Dict[str, Any]]): Keyword 
                arguments where each key is a file_type (also the attribute name 
                to be created) and each value is a dict of parameters specific to 
                that sub-dict. These are merged with shared_subdicts_params when 
                constructing the sub-dict. The resulting dict also receives 
                file_type=<key>.

        Raises:
            TypeError: If dict_type is not a PersiDict subclass, or if
                shared_subdicts_params is not a dict, or if any individual
                parameter set is not a dict.
        """
        if not issubclass(dict_type, PersiDict):
            raise TypeError("dict_type must be a subclass of PersiDict")
        if not isinstance(shared_subdicts_params, dict):
            raise TypeError("shared_subdicts_params must be a dict")
        self.dict_type = dict_type
        self.shared_subdicts_params = shared_subdicts_params
        self.individual_subdicts_params = individual_subdicts_params
        self.subdicts_names = list(individual_subdicts_params.keys())
        for subdict_name in individual_subdicts_params:
            if not isinstance(individual_subdicts_params[subdict_name], dict):
                raise TypeError(
                    f"Params for subdict {subdict_name!r} must be a dict")
            self.__dict__[subdict_name] = dict_type(
                **{**shared_subdicts_params,
                   **individual_subdicts_params[subdict_name],
                   "file_type": subdict_name})

    def __getstate__(self):
        """Prevent pickling.

        Raises:
            TypeError: Always raised; this object is not pickleable.
        """
        raise TypeError("OverlappingMultiDict cannot be pickled.")

    def __setstate__(self, state):
        """Prevent unpickling.

        Args:
            state: The state dictionary that would be used for unpickling (ignored).

        Raises:
            TypeError: Always raised; this object is not pickleable.
        """
        raise TypeError("OverlappingMultiDict cannot be pickled.")

    def __getitem__(self, key):
        """Disallow item access on the container itself.

        Suggest accessing items through the sub-dictionaries exposed as
        attributes (e.g., obj.json[key]).

        Args:
            key: The key that would be accessed (ignored).

        Raises:
            TypeError: Always raised to indicate unsupported operation.
        """
        raise TypeError(
            "OverlappingMultiDict does not support item access by key. "
            "Individual items should be accessed through nested dicts, "
            f"which are available via attributes {self.subdicts_names}")

    def __setitem__(self, key, value):
        """Disallow item assignment on the container itself.

        Args:
            key: The key that would be assigned (ignored).
            value: The value that would be assigned (ignored).

        Raises:
            TypeError: Always raised to indicate unsupported operation.
        """
        raise TypeError(
            "OverlappingMultiDict does not support item assignment by key. "
            "Individual items should be accessed through nested dicts, "
             f"which are available via attributes {self.subdicts_names}")

    def __delitem__(self, key):
        """Disallow item deletion on the container itself.

        Args:
            key: The key that would be deleted (ignored).

        Raises:
            TypeError: Always raised to indicate unsupported operation.
        """
        raise TypeError(
            "OverlappingMultiDict does not support item deletion by key. "
            "Individual items can be deleted through nested dicts, "
            f"which are available via attributes {self.subdicts_names}")