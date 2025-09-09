import string

SAFE_CHARS_SET = set(string.ascii_letters + string.digits + "()_-~.=")
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
