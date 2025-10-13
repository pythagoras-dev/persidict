"""Unit tests for the version attribute of the persidict package."""
import re
import persidict


def test_version_exists():
    """Test that __version__ attribute exists."""
    assert hasattr(persidict, '__version__')


def test_version_is_string():
    """Test that __version__ is a string."""
    assert isinstance(persidict.__version__, str)


def test_version_not_empty():
    """Test that __version__ is not empty."""
    assert len(persidict.__version__) > 0


def test_version_format():
    """Test that __version__ follows semantic versioning format (X.Y.Z or X.Y.Z.something)."""
    version_pattern = r'^\d+\.\d+\.\d+(?:\.\w+|\w+\d*)?$'
    assert re.match(version_pattern, persidict.__version__), f"Version '{persidict.__version__}' does not match expected format"


def test_version_accessible():
    """Test that __version__ can be accessed and printed."""
    version = persidict.__version__
    assert version is not None
    # Should not raise any exceptions when converted to string
    str_version = str(version)
    assert len(str_version) > 0


def test_version_matches_metadata():
    import persidict
    from importlib import metadata
    assert persidict.__version__ == metadata.version("persidict")