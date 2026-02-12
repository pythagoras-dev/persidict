"""Tests for storing and retrieving standard library atomic types in PersiDict."""

import datetime
import decimal
import enum
import fractions
import ipaddress
import pathlib
import re
import uuid

import pytest

from ..atomic_test_config import atomic_type_tests, make_test_dict


class SampleEnum(enum.Enum):
    """Sample enum for testing."""
    VALUE_A = 1
    VALUE_B = 2


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pathlib_path(tmp_path, DictToTest):
    """Verify pathlib.Path values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = pathlib.Path("/tmp/test/file.txt")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_pathlib_purepath(tmp_path, DictToTest):
    """Verify pathlib.PurePath values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = pathlib.PurePosixPath("/pure/path/file.txt")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_datetime_datetime(tmp_path, DictToTest):
    """Verify datetime.datetime values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = datetime.datetime(2024, 1, 15, 12, 30, 45, 123456)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_datetime_date(tmp_path, DictToTest):
    """Verify datetime.date values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = datetime.date(2024, 1, 15)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_datetime_time(tmp_path, DictToTest):
    """Verify datetime.time values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = datetime.time(12, 30, 45, 123456)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_datetime_timedelta(tmp_path, DictToTest):
    """Verify datetime.timedelta values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = datetime.timedelta(days=5, hours=3, minutes=30)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_datetime_timezone(tmp_path, DictToTest):
    """Verify datetime.timezone values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_decimal_decimal(tmp_path, DictToTest):
    """Verify decimal.Decimal values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = decimal.Decimal("123.456789")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_fractions_fraction(tmp_path, DictToTest):
    """Verify fractions.Fraction values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = fractions.Fraction(3, 7)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_uuid_uuid(tmp_path, DictToTest):
    """Verify uuid.UUID values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = uuid.UUID("12345678-1234-5678-1234-567812345678")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_re_pattern(tmp_path, DictToTest):
    """Verify re.Pattern values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = re.compile(r"\w+@\w+\.\w+")
    d["key"] = original
    retrieved = d["key"]
    assert retrieved.pattern == original.pattern

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_enum_enum(tmp_path, DictToTest):
    """Verify enum.Enum values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    d["key"] = SampleEnum.VALUE_A
    assert d["key"] == SampleEnum.VALUE_A

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_range_type(tmp_path, DictToTest):
    """Verify range values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = range(10, 100, 5)
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_ipaddress_ipv4address(tmp_path, DictToTest):
    """Verify ipaddress.IPv4Address values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = ipaddress.IPv4Address("192.168.1.1")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_ipaddress_ipv6address(tmp_path, DictToTest):
    """Verify ipaddress.IPv6Address values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = ipaddress.IPv6Address("::1")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_ipaddress_ipv4network(tmp_path, DictToTest):
    """Verify ipaddress.IPv4Network values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = ipaddress.IPv4Network("192.168.1.0/24")
    d["key"] = original
    assert d["key"] == original

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_ipaddress_ipv6network(tmp_path, DictToTest):
    """Verify ipaddress.IPv6Network values can be stored and retrieved."""
    d = make_test_dict(DictToTest, tmp_path)
    d.clear()

    original = ipaddress.IPv6Network("2001:db8::/32")
    d["key"] = original
    assert d["key"] == original

    d.clear()
