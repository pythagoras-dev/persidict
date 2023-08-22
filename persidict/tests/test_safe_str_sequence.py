from persidict import SafeStrSequence, sign_safe_str_sequence, unsign_safe_str_sequence

def test_add():
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrSequence(*l1)
    s2 = SafeStrSequence(*l2)
    assert s1 + s2 == SafeStrSequence(*l1, *l2)
    assert s2 + s1 == SafeStrSequence(*l2, *l1)

def test_radd():
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrSequence(*l1)
    s2 = SafeStrSequence(*l2)
    assert s1 + s2 == SafeStrSequence(*l1, *l2)
    assert s2 + s1 == SafeStrSequence(*l2, *l1)

def test_eq():
    l1 = ['a', 'b', 'c']
    l2 = ['d', 'e', 'f']
    s1 = SafeStrSequence(*l1)
    s2 = SafeStrSequence(*l2)
    assert s1 == SafeStrSequence(*l1)
    assert s2 == SafeStrSequence(*l2)
    assert s1 != s2

def test_getitem():
    l = ['a', 'b', 'c', 'd', 'e', 'x', 'y', 'z']
    s = SafeStrSequence(*l)
    for i, c in enumerate(s):
        assert s[i] == l[i] == c
    assert s == SafeStrSequence(s)

def test_len():
    l = ['a', 'b', 'c']
    s = SafeStrSequence(*l)
    assert len(s) == len(l)

def test_contains():
    l = ['a', 'b', 'c']
    s = SafeStrSequence(*l)
    for c in l:
        assert c in s
        assert c*10 not in s

def test_reversed():
    l = ['a', 'b', 'c', 'x', 'y', 'z']
    s = SafeStrSequence(*l)
    assert s == reversed(reversed(s))
    assert s != reversed(s)

def test_count():
    l = ['a', 'b', 'c', 'a']
    s = SafeStrSequence(*l)
    for c in l:
        assert s.count(c) == l.count(c)
        assert s.count(c*100) == 0

def test_init():
    l = ['a', 'b', 'c']
    s = SafeStrSequence(*l)
    assert s == SafeStrSequence(s)
    assert s == SafeStrSequence(*l)
    assert s != reversed(s)
    assert s != SafeStrSequence(*l, 'd')
    assert s != SafeStrSequence(*l, 'd', 'e')
    assert s != SafeStrSequence(*l, 'd', 'e', 'f')

def test_signing_unsigning():
    l = ['a', 'b', 'c']
    for n in range(4,20):
        s = SafeStrSequence(*l)
        signed_s = sign_safe_str_sequence(s, n)
        assert s == unsign_safe_str_sequence(signed_s, n)
        assert s != signed_s
        assert signed_s == sign_safe_str_sequence(signed_s, n)
        assert s == unsign_safe_str_sequence(s, n)

def test_unsafe_chars():
    """Test if SafeStrSequence rejects unsafe characters."""
    bad_chars = ['\n', '\t', '\r', '\b', '\x0b']
    bad_chars += [ '\x0c', '\x1c', '\x1d', '\x1e', '\x1f']

    for c in bad_chars:
        try:
            SafeStrSequence("qwerty"+c+"uiop")
        except:
            pass
        else:
            assert False, f"Failed to reject unsafe character {c}"

def test_flattening():
    """Test if SafeStrSequence flattens nested sequences."""
    l_1 = ['a', 'b', 'c']
    l_2 = ['d', 'e', ('f','g'), 'h']
    l_3 = ['i', 'j', ['k', 'l', ('m',('n','o')) ]]
    s = SafeStrSequence(l_1, SafeStrSequence(l_2), l_3)
    assert "".join(s.safe_strings) == "abcdefghijklmno"

def test_rejecting_non_strings():
    """Test if SafeStrSequence rejects non-string elements."""
    bad_args = [1, 2.0, 3+4j, None, True, False, object(), dict(), set()]
    print("\n")
    for a in bad_args:
        print(f"{type(a)},{a}")
        try:
            SafeStrSequence(a)
        except:
            pass
        else:
            assert False, f"Failed to reject non-string argument {a}"

def test_rejecting_empty_strings():
    """Test if SafeStrSequence rejects empty strings."""
    try:
        SafeStrSequence("")
    except:
        pass
    else:
        assert False, "Failed to reject empty string"

def test_rejecting_empty_sequences():
    """Test if SafeStrSequence rejects empty sequences."""
    try:
        SafeStrSequence()
    except:
        pass
    else:
        assert False, "Failed to reject empty sequence"
