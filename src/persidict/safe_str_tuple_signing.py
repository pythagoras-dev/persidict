"""Functions for signing and unsigning SafeStrTuples.

Two functions from this module allow to add and remove
hash signature suffixes to/from all strings in a SafeStrTuple:
sign_safe_str_tuple() and unsign_safe_str_tuple().

The suffixes are used to ensure correct work of persistent dictionaries
(which employ SafeStrTuple-s as keys) with case-insensitive filesystems,
e.g. MacOS HFS.
"""

import base64
import hashlib
from .safe_str_tuple import SafeStrTuple


def _create_signature_suffix(input_str:str, digest_len:int) -> str:
    """ Create a hash signature suffix for a string."""

    assert isinstance(input_str, str)
    assert isinstance(digest_len, int)
    assert digest_len >= 0

    if digest_len == 0:
        return ""

    input_b = input_str.encode()
    hash_object = hashlib.md5(input_b)
    full_digest_str = base64.b32encode(hash_object.digest()).decode()
    suffix = "_" + full_digest_str[:digest_len].lower()

    return suffix


def _add_signature_suffix_if_absent(input_str:str, digest_len:int) -> str:
    """ Add a hash signature suffix to a string if it's not there."""

    assert isinstance(input_str, str)
    assert isinstance(digest_len, int)
    assert digest_len >= 0

    if digest_len == 0:
        return input_str

    if len(input_str) > digest_len + 1:
        possibly_already_present_suffix = _create_signature_suffix(
            input_str[:-1-digest_len], digest_len)
        if input_str.endswith(possibly_already_present_suffix):
            return input_str

    return input_str + _create_signature_suffix(input_str, digest_len)


def _add_all_suffixes_if_absent(
        str_seq:SafeStrTuple
        ,digest_len:int
        ) -> SafeStrTuple:
    """Add hash signature suffixes to all strings in a SafeStrTuple."""

    str_seq = SafeStrTuple(str_seq)

    new_seq = []
    for s in str_seq:
        new_seq.append(_add_signature_suffix_if_absent(s,digest_len))

    new_seq = SafeStrTuple(*new_seq)

    return new_seq


def _remove_signature_suffix_if_present(input_str:str, digest_len:int) -> str:
    """ Remove a hash signature suffix from a string if it's detected."""

    assert isinstance(input_str, str)
    assert isinstance(digest_len, int)
    assert digest_len >= 0

    if digest_len == 0:
        return input_str

    if len(input_str) > digest_len + 1:
        possibly_already_present_suffix = _create_signature_suffix(
            input_str[:-1-digest_len], digest_len)
        if input_str.endswith(possibly_already_present_suffix):
            return input_str[:-1-digest_len]

    return input_str


def _remove_all_signature_suffixes_if_present(
        str_seq:SafeStrTuple
        , digest_len:int
        ) -> SafeStrTuple:
    """Remove hash signature suffixes from all strings in a SafeStrTuple."""

    str_seq = SafeStrTuple(str_seq)

    if digest_len == 0:
        return str_seq

    new_seq = []
    for s in str_seq:
        new_string = _remove_signature_suffix_if_present(s, digest_len)
        new_seq.append(new_string)

    new_seq = SafeStrTuple(*new_seq)

    return new_seq


def sign_safe_str_tuple(str_seq:SafeStrTuple
                        , digest_len:int
                        ) -> SafeStrTuple:
    """Add hash signature suffixes to all strings in a SafeStrTuple."""

    str_seq = SafeStrTuple(str_seq)

    str_seq = _add_all_suffixes_if_absent(str_seq, digest_len)

    return str_seq


def unsign_safe_str_tuple(str_seq:SafeStrTuple
                          , digest_len:int
                          ) -> SafeStrTuple:
    """Remove hash signature suffixes from all strings in a SafeStrTuple."""

    str_seq = SafeStrTuple(str_seq)

    str_seq = _remove_all_signature_suffixes_if_present(str_seq, digest_len)

    return str_seq