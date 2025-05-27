import string
from copy import deepcopy

SAFE_CHARS_SET = set(string.ascii_letters + string.digits + "()_-~.=")
SAFE_STRING_MAX_LENGTH = 254

def get_safe_chars() -> set[str]:
    """Return a set of allowed characters."""
    return deepcopy(SAFE_CHARS_SET)

def replace_unsafe_chars(a_str:str, replace_with:str) -> str :
    """ Replace unsafe (special) characters with allowed (safe) ones."""
    safe_chars = get_safe_chars()
    result_list = [(c if c in safe_chars else replace_with) for c in a_str]
    result_str = "".join(result_list)
    return result_str
