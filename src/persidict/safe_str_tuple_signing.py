"""Sign and unsign SafeStrTuple elements with deterministic suffixes.

This module provides helpers to add or remove short, deterministic hash
suffixes to every string element of a SafeStrTuple. These suffixes are used
to avoid collisions on case-insensitive filesystems (e.g., macOS HFS) while
keeping keys stable and portable.

Functions:
    sign_safe_str_tuple(str_seq, digest_len):
        Return a new SafeStrTuple where each element is suffixed with an
        underscore and a base32-encoded MD5 digest fragment of length
        ``digest_len``. If a correct suffix is already present, it is not
        duplicated.
    unsign_safe_str_tuple(str_seq, digest_len):
        Return a new SafeStrTuple where a previously added suffix of length
        ``digest_len`` is removed from each element, when detected.
"""

import base64
import hashlib
from .safe_str_tuple import SafeStrTuple


def _create_signature_suffix(input_str: str, digest_len: int) -> str:
    """Create a short, deterministic hash suffix for a string.

    The suffix format is ``_<b32(md5(input))[:digest_len].lower()>``. For
    ``digest_len == 0`` an empty string is returned.

    Args:
        input_str: Input string to sign.
        digest_len: Number of base32 characters from the MD5 digest to include.
            Must be non-negative. A value of 0 disables suffixing.

    Returns:
        str: The computed suffix to append (may be an empty string).

    Raises:
        TypeError: If input_str is not a str or digest_len is not an int.
        ValueError: If digest_len is negative.
    """

    if not isinstance(input_str, str):
        raise TypeError(f"input_str must be str, got {type(input_str)!r}")
    if not isinstance(digest_len, int):
        raise TypeError(f"digest_len must be int, got {type(digest_len)!r}")
    if digest_len < 0:
        raise ValueError(f"digest_len must be >= 0, got {digest_len}")

    if digest_len == 0:
        return ""

    input_b = input_str.encode()
    hash_object = hashlib.md5(input_b)
    full_digest_str = base64.b32encode(hash_object.digest()).decode()
    suffix = "_" + full_digest_str[:digest_len].lower()

    return suffix


def _add_signature_suffix_if_absent(input_str: str, digest_len: int) -> str:
    """Add the hash signature suffix if it's not already present.

    If the input already ends with the exact suffix calculated from its
    unsuffixed part, it is returned unchanged.

    Args:
        input_str: The string to sign.
        digest_len: Length of the digest fragment to use; 0 leaves the string
            unchanged.

    Returns:
        str: The original or suffixed string.

    Raises:
        TypeError: If input_str is not a str or digest_len is not an int.
        ValueError: If digest_len is negative.
    """

    if not isinstance(input_str, str):
        raise TypeError(f"input_str must be str, got {type(input_str)!r}")
    if not isinstance(digest_len, int):
        raise TypeError(f"digest_len must be int, got {type(digest_len)!r}")
    if digest_len < 0:
        raise ValueError(f"digest_len must be >= 0, got {digest_len}")

    if digest_len == 0:
        return input_str

    if len(input_str) > digest_len + 1:
        possibly_already_present_suffix = _create_signature_suffix(
            input_str[:-1-digest_len], digest_len)
        if input_str.endswith(possibly_already_present_suffix):
            return input_str

    return input_str + _create_signature_suffix(input_str, digest_len)


def _add_all_suffixes_if_absent(
        str_seq: SafeStrTuple,
        digest_len: int,
        ) -> SafeStrTuple:
    """Return a new SafeStrTuple with suffixes added to each element.

    Args:
        str_seq: Input sequence convertible to SafeStrTuple.
        digest_len: Digest fragment length; 0 results in a no-op.

    Returns:
        SafeStrTuple: The suffixed sequence.
    """

    str_seq = SafeStrTuple(str_seq)

    new_seq = []
    for s in str_seq:
        new_seq.append(_add_signature_suffix_if_absent(s, digest_len))

    new_seq = SafeStrTuple(*new_seq)

    return new_seq


def _remove_signature_suffix_if_present(input_str: str, digest_len: int) -> str:
    """Remove the hash signature suffix if it is detected.

    Detection is performed by recomputing the expected suffix from the
    unsuffixed portion and comparing it to the current ending.

    Args:
        input_str: The possibly suffixed string.
        digest_len: Digest fragment length used during signing; 0 leaves the
            string unchanged.

    Returns:
        str: The original string without the suffix if detected; otherwise the
        original string.

    Raises:
        TypeError: If input_str is not a str or digest_len is not an int.
        ValueError: If digest_len is negative.
    """

    if not isinstance(input_str, str):
        raise TypeError(f"input_str must be str, got {type(input_str)!r}")
    if not isinstance(digest_len, int):
        raise TypeError(f"digest_len must be int, got {type(digest_len)!r}")
    if digest_len < 0:
        raise ValueError(f"digest_len must be >= 0, got {digest_len}")

    if digest_len == 0:
        return input_str

    if len(input_str) > digest_len + 1:
        possibly_already_present_suffix = _create_signature_suffix(
            input_str[:-1-digest_len], digest_len)
        if input_str.endswith(possibly_already_present_suffix):
            return input_str[:-1-digest_len]

    return input_str


def _remove_all_signature_suffixes_if_present(
        str_seq: SafeStrTuple,
        digest_len: int,
        ) -> SafeStrTuple:
    """Return a new SafeStrTuple with detected suffixes removed from elements.

    Args:
        str_seq: Input sequence convertible to SafeStrTuple.
        digest_len: Digest fragment length used during signing; 0 results in a
            no-op.

    Returns:
        SafeStrTuple: The unsigned sequence.
    """

    str_seq = SafeStrTuple(str_seq)

    if digest_len == 0:
        return str_seq

    new_seq = []
    for s in str_seq:
        new_string = _remove_signature_suffix_if_present(s, digest_len)
        new_seq.append(new_string)

    new_seq = SafeStrTuple(*new_seq)

    return new_seq


def sign_safe_str_tuple(
                        str_seq: SafeStrTuple,
                        digest_len: int,
                        ) -> SafeStrTuple:
    """Return a SafeStrTuple with signature suffixes added to all elements.

    This is the public function for signing keys used by persistent dicts.

    Args:
        str_seq: Input sequence convertible to SafeStrTuple.
        digest_len: Number of characters from the base32 digest to append. Use
            0 to disable suffixing.

    Returns:
        SafeStrTuple: The suffixed sequence.
    """

    str_seq = SafeStrTuple(str_seq)

    str_seq = _add_all_suffixes_if_absent(str_seq, digest_len)

    return str_seq


def unsign_safe_str_tuple(
                          str_seq: SafeStrTuple,
                          digest_len: int,
                          ) -> SafeStrTuple:
    """Return a SafeStrTuple with detected signature suffixes removed.

    Args:
        str_seq: Input sequence convertible to SafeStrTuple.
        digest_len: Number of characters that were appended during signing. Use
            0 for a no-op.

    Returns:
        SafeStrTuple: The unsigned sequence.
    """

    str_seq = SafeStrTuple(str_seq)

    str_seq = _remove_all_signature_suffixes_if_present(str_seq, digest_len)

    return str_seq