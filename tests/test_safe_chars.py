from persidict.safe_chars import replace_unsafe_chars, SAFE_CHARS_SET

def test_replace_unsafe_chars():
    assert replace_unsafe_chars("a@b*c", "_") == "a_b_c"
    assert replace_unsafe_chars("X Y&Z", "") == "XYZ"

def test_safe_chars_set():
    assert isinstance(SAFE_CHARS_SET, set)
    assert len(SAFE_CHARS_SET) >= 52
    assert len(SAFE_CHARS_SET & set("\t\n\r\\/")) == 0