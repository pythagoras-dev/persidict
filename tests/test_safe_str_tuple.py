from persidict.safe_chars import SAFE_CHARS_SET, get_safe_chars
from persidict import SafeStrTuple, NonEmptySafeStrTuple
from persidict.safe_str_tuple_signing import sign_safe_str_tuple, unsign_safe_str_tuple

def test_add():
    """Test if SafeStrTuple concatenates correctly."""
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrTuple(*l1)
    s2 = SafeStrTuple(*l2)
    assert s1 + s2 == SafeStrTuple(*l1, *l2)
    assert s2 + s1 == SafeStrTuple(*l2, *l1)

def test_radd():
    """Test if SafeStrTuple concatenates correctly."""
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrTuple(*l1)
    s2 = SafeStrTuple(*l2)
    assert s1 + s2 == SafeStrTuple(*l1, *l2)
    assert s2 + s1 == SafeStrTuple(*l2, *l1)

def test_eq():
    """Test if SafeStrTuple compares correctly."""
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrTuple(*l1)
    s2 = SafeStrTuple(*l2)
    assert s1 == SafeStrTuple(*l1)
    assert s2 == SafeStrTuple(*l2)
    assert s1 != s2

def test_getitem():
    """Test if SafeStrTuple gets items correctly."""
    l = ['a', 'b', 'c', 'd', 'e', 'x', 'y', 'z']
    s = SafeStrTuple(*l)
    for i, c in enumerate(s):
        assert s[i] == l[i] == c
    assert s == SafeStrTuple(s)

def test_len():
    """Test if SafeStrTuple gets length correctly."""
    l = ['a', 'b', 'c']
    s = SafeStrTuple(*l)
    assert len(s) == len(l)

def test_contains():
    """Test if SafeStrTuple checks containment correctly."""
    l = ['a', 'b', 'c']
    s = SafeStrTuple(*l)
    for c in l:
        assert c in s
        assert c*10 not in s

def test_reversed():
    l = ['a', 'b', 'c', 'x', 'y', 'z']
    s = SafeStrTuple(*l)
    assert s == reversed(reversed(s))
    assert s != reversed(s)

def test_count():
    l = ['a', 'b', 'c', 'a']
    s = SafeStrTuple(*l)
    for c in l:
        assert s.count(c) == l.count(c)
        assert s.count(c*100) == 0

def test_init():
    l = ['a', 'b', 'c']
    s = SafeStrTuple(*l)
    assert s == SafeStrTuple(s)
    assert s == SafeStrTuple(*l)
    assert s != reversed(s)
    assert s != SafeStrTuple(*l, 'd')
    assert s != SafeStrTuple(*l, 'd', 'e')
    assert s != SafeStrTuple(*l, 'd', 'e', 'f')

def test_signing_unsigning():
    l = ['a', 'b', 'c']
    for n in range(0,20):
        s = SafeStrTuple(*l)
        signed_s = sign_safe_str_tuple(s, n)
        assert s == unsign_safe_str_tuple(signed_s, n)
        if n > 0:
            assert s != signed_s
        else:
            assert s == signed_s
        assert signed_s == sign_safe_str_tuple(signed_s, n)
        assert s == unsign_safe_str_tuple(s, n)

def test_unsafe_chars():
    """Test if SafeStrTuple rejects unsafe characters."""
    bad_chars = ['\n', '\t', '\r', '\b', '\x0b']
    bad_chars += [ '\x0c', '\x1c', '\x1d', '\x1e', '\x1f']

    for c in bad_chars:
        try:
            SafeStrTuple("qwerty" + c + "uiop")
        except:
            pass
        else:
            assert False, f"Failed to reject unsafe character {c}"

def test_flattening():
    """Test if SafeStrTuple flattens nested sequences."""
    l_1 = ['a', 'b', 'c']
    l_2 = ['d', 'e', ('f','g'), 'h']
    l_3 = ['i', 'j', ['k', 'l', ('m',('n','o')) ]]
    s = SafeStrTuple(l_1, SafeStrTuple(l_2), l_3)
    assert "".join(s.strings) == "abcdefghijklmno"

def test_rejecting_non_strings():
    """Test if SafeStrTuple rejects non-string elements."""
    bad_args = [1, 2.0, 3+4j, None, True, False, object(), dict(), set()]
    for a in bad_args:
        try:
            SafeStrTuple(a)
        except:
            pass
        else:
            assert False, f"Failed to reject non-string argument {a}"

def test_rejecting_empty_strings():
    """Test if SafeStrTuple rejects empty strings."""
    try:
        SafeStrTuple("")
    except:
        pass
    else:
        assert False, "Failed to reject empty string"

def test_rejecting_empty_sequences():
    """Test if SafeStrTuple rejects empty sequences."""
    SafeStrTuple([])
    try:
        NonEmptySafeStrTuple([])
    except:
        pass
    else:
        assert False, "Failed to reject empty sequence"

def test_get_safe_chars():
    """Test if get_safe_chars() returns a copy of SAFE_CHARS_SET."""
    assert get_safe_chars() == SAFE_CHARS_SET

def test_eq_overrideability():
    class BadTuple(SafeStrTuple):
        def __eq__(self, other):
            return False

    class GoodTuple(SafeStrTuple):
        def __eq__(self, other):
            return self.strings == other.strings

    class GoodTuple2(SafeStrTuple):
        pass

    safe_tuple = SafeStrTuple("a", "b", "c")
    bad_tuple = BadTuple(safe_tuple)
    good_tuple = GoodTuple(safe_tuple)
    good_tuple_2 = GoodTuple2(safe_tuple)

    assert (safe_tuple == bad_tuple) == False
    assert (bad_tuple == safe_tuple) == False
    assert (safe_tuple != bad_tuple) == True
    assert (bad_tuple != safe_tuple) == True

    assert (good_tuple_2 == bad_tuple) == False
    assert (bad_tuple == good_tuple_2) == False
    assert (good_tuple_2 != bad_tuple) == True
    assert (bad_tuple != good_tuple_2) == True

    assert (safe_tuple == good_tuple) == True
    assert (good_tuple == safe_tuple) == True
    assert (safe_tuple != good_tuple) == False
    assert (good_tuple != safe_tuple) == False

    assert (safe_tuple == good_tuple_2) == True
    assert (good_tuple_2 == safe_tuple) == True
    assert (safe_tuple != good_tuple_2) == False
    assert (good_tuple_2 != safe_tuple) == False

    assert (good_tuple == good_tuple_2) == True
    assert (good_tuple_2 == good_tuple) == True
    assert (good_tuple != good_tuple_2) == False
    assert (good_tuple_2 != good_tuple) == False
