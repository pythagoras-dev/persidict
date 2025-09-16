"""Safe character handling utilities for URL and filesystem compatibility.

This module defines character sets and length constraints for building strings
that are safe for use in URLs, filenames, and other contexts where character
restrictions apply.
"""
import string

# Set of characters considered safe for filenames and URL components.
# Includes ASCII letters (a-z, A-Z), digits (0-9), and special chars: ()_-~.=
SAFE_CHARS_SET = set(string.ascii_letters + string.digits + "()_-~.=")

# Maximum length for safe strings to ensure compatibility with various filesystems
# and URL length limitations. Set to 254 to stay well under most system limits.
SAFE_STRING_MAX_LENGTH = 254

def get_safe_chars() -> set[str]:
    """Get the set of allowed characters.

    Returns:
        set[str]: A copy of the set of characters considered safe for
            building file names and URL components. Includes ASCII letters,
            digits, and the characters ()_-~.= .
    """
    return SAFE_CHARS_SET.copy()

def replace_unsafe_chars(a_str: str, replace_with: str) -> str:
    """Replace unsafe characters in a string.

    Replaces any character not present in the safe-character set with a
    replacement substring.

    Args:
        a_str (str): Input string that may contain unsafe characters.
        replace_with (str): The substring to use for every unsafe character
            encountered in a_str.

    Returns:
        str: The transformed string where all unsafe characters are replaced
        by the provided replacement substring.
    """
    safe_chars = get_safe_chars()
    result_list = [(c if c in safe_chars else replace_with) for c in a_str]
    result_str = "".join(result_list)
    return result_str

def contains_unsafe_chars(a_str: str) -> bool:
    """Check if a string contains unsafe characters.

    Args:
        a_str (str): Input string to check for unsafe characters.

    Returns:
        bool: True if the string contains any character not in the safe
            character set, False otherwise.
    """
    safe_chars = get_safe_chars()
    return any(c not in safe_chars for c in a_str)
