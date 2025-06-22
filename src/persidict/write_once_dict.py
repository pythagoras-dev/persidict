from __future__ import annotations

import time

from deepdiff import DeepDiff
from parameterizable import register_parameterizable_class, sort_dict_by_keys

from .jokers import KEEP_CURRENT, KeepCurrentFlag
from .persi_dict import PersiDict
from .file_dir_dict import FileDirDict
import random
import sys
from typing import Any

import joblib.hashing
from .persi_dict import PersiDictKey

def _get_md5_signature(x:Any) -> str:
    """Return base16 MD5 hash signature of an object.

    Uses joblib's Hasher (or NumpyHasher). It uses Pickle for serialization,
    except for NumPy arrays, which use optimized custom routines.
    """
    if 'numpy' in sys.modules:
        hasher = joblib.hashing.NumpyHasher(hash_name='md5')
    else:
        hasher = joblib.hashing.Hasher(hash_name='md5')
    hash_signature = hasher.hash(x)
    return str(hash_signature)

class WriteOnceDict(PersiDict):
    """ A dictionary that always keeps the first value assigned to a key.

    If a key is already set, it randomly checks the value against the value
    that was first set. If the new value is different, it raises a
    ValueError exception. Once can control the frequency of these checks
    or even completely disable them by setting `p_consistency_checks` to 0.

    """
    _wrapped_dict: PersiDict
    _p_consistency_checks: float | None
    _consistency_checks_attempted: int
    _consistency_checks_passed: int

    def __init__(self
                 , wrapped_dict:PersiDict | None = None
                 , p_consistency_checks: float | None=None):
        if wrapped_dict is None:
            wrapped_dict = FileDirDict(immutable_items = True)
        assert isinstance(wrapped_dict, PersiDict)
        assert wrapped_dict.immutable_items == True
        self.p_consistency_checks = p_consistency_checks
        PersiDict.__init__(self
            , base_class_for_values=wrapped_dict.base_class_for_values
            , immutable_items=True
            , digest_len=wrapped_dict.digest_len)
        self._wrapped_dict = wrapped_dict
        self._consistency_checks_passed = 0
        self._consistency_checks_attempted = 0


    @property
    def p_consistency_checks(self) -> float:
        """ Probability of checking the value against the first value set. """
        return self._p_consistency_checks


    @p_consistency_checks.setter
    def p_consistency_checks(self, value: float|None|KeepCurrentFlag) -> None:
        if value is KEEP_CURRENT:
            if hasattr(self, '_p_consistency_checks'):
                return
            else:
                raise ValueError(
                    f"KEEP_CURRENT can't be used to initialize p_consistency_checks.")
        if value is None:
            value = 0.0
        if not (0 <= value <= 1):
            raise ValueError(
                f"p_consistency_checks must be in [0, 1], "
                f"got {value}.")
        self._p_consistency_checks = value


    @property
    def consistency_checks_failed(self) -> int:
        """ Returns the number of failed consistency checks. """
        return self._consistency_checks_attempted - self._consistency_checks_passed


    @property
    def consistency_checks_attempted(self) -> int:
        """ Returns the number of attempted consistency checks. """
        return self._consistency_checks_attempted


    @property
    def consistency_checks_passed(self) -> int:
        """ Returns the number of successful consistency checks. """
        return self._consistency_checks_passed


    def get_params(self):
        params = dict(
            wrapped_dict = self._wrapped_dict,
            p_consistency_checks = self.p_consistency_checks)
        sorted_params = sort_dict_by_keys(params)
        return sorted_params

    def __setitem__(self, key, value):
        """ Set the value of a key if it is not already set.

        If the key is already set, it checks the value
        against the value that was first set.
        """
        check_needed = False

        try: # extra protections to better handle concurrent writes
            if key in self._wrapped_dict:
                check_needed = True
            else:
                self._wrapped_dict[key] = value
        except:
            time.sleep(random.random()/random.randint(1,5))
            if key in self._wrapped_dict:
                check_needed = True
            else:
                self._wrapped_dict[key] = value

        if not key in self._wrapped_dict:
            raise KeyError(
                f"Key {key} was not set in the wrapped dict "
                + f"{self._wrapped_dict}. This should not happen.")

        if check_needed and self.p_consistency_checks > 0:
            if random.random() < self.p_consistency_checks:
                self._consistency_checks_attempted += 1
                signature_old = _get_md5_signature(self._wrapped_dict[key])
                signature_new = _get_md5_signature(value)
                if signature_old != signature_new:
                    diff_dict = DeepDiff(self._wrapped_dict[key], value)
                    raise ValueError(
                        f"Key {key} is already set "
                        + f"to {self._wrapped_dict[key]} "
                        + f"and the new value {value} is different, "
                        + f"which is not allowed. Details here: {diff_dict} ")
                self._consistency_checks_passed += 1

    def __contains__(self, item):
        return item in self._wrapped_dict

    def __getitem__(self, key):
        return self._wrapped_dict[key]

    def __len__(self):
        return len(self._wrapped_dict)

    def _generic_iter(self, iter_type: str):
        return self._wrapped_dict._generic_iter(iter_type)

    def timestamp(self, key:PersiDictKey) -> float:
        return self._wrapped_dict.timestamp(key)

    def __getattr__(self, name):
        # Forward attribute access to the wrapped object
        return getattr(self._wrapped_dict, name)

    @property
    def base_dir(self):
        return self._wrapped_dict.base_dir

    @property
    def base_url(self):
        return self._wrapped_dict.base_url

    def get_subdict(self, prefix_key:PersiDictKey) -> WriteOnceDict:
        subdict = self._wrapped_dict.get_subdict(prefix_key)
        result = WriteOnceDict(subdict, self.p_consistency_checks)
        return result

register_parameterizable_class(WriteOnceDict)