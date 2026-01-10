"""Live-on-self CI/CD "test" for cache clearing functionality.

This test runs against the actual project to ensure that cache clearing works
correctly on the real project structure. It's a CI/CD test that validates the
cache cleaning functionality works as expected while actually removing the
cache files.

The test fails if:
1. pyproject.toml is not found at project root
2. remove_python_cache_files() raises an exception
3. Function returns invalid data structure

The test passes whether or not cache files are found (idempotent behavior).
"""
import pytest
from pathlib import Path

from mixinforge.command_line_tools.basic_file_utils import (
    remove_python_cache_files,
    folder_contains_pyproject_toml,
    format_cache_statistics
)


@pytest.mark.live_actions
def test_live_clear_cache(pytestconfig):
    """Test that cache clearing works on the actual project.

    This is a live-on-self test that validates:
    - pyproject.toml exists at project root
    - remove_python_cache_files() executes without errors
    - Function returns valid tuple structure (count, list)
    - Cache removal works on real project structure
    - Test is idempotent (passes with or without caches)

    Args:
        pytestconfig: Pytest config fixture providing rootdir.
    """
    # Get project root from pytest's rootdir
    project_root = Path(pytestconfig.rootdir)

    # Validate project root contains pyproject.toml
    assert folder_contains_pyproject_toml(project_root), \
        f"pyproject.toml not found at project root: {project_root}"

    # Execute cache clearing on actual project
    try:
        result = remove_python_cache_files(project_root)
    except Exception as e:
        pytest.fail(f"remove_python_cache_files() raised exception: {e}")

    # Validate return structure
    assert isinstance(result, tuple), \
        f"Expected tuple return, got {type(result)}"
    assert len(result) == 2, \
        f"Expected tuple of length 2, got {len(result)}"

    removed_count, removed_items = result

    # Validate return types
    assert isinstance(removed_count, int), \
        f"Expected count to be int, got {type(removed_count)}"
    assert isinstance(removed_items, list), \
        f"Expected items to be list, got {type(removed_items)}"

    # Count must be non-negative
    assert removed_count >= 0, \
        f"Expected non-negative count, got {removed_count}"

    # If items were removed, verify list consistency
    if removed_count > 0:
        assert len(removed_items) == removed_count, \
            f"Count mismatch: count={removed_count}, list length={len(removed_items)}"
        assert all(isinstance(item, str) for item in removed_items), \
            "All removed items should be strings (relative paths)"

        # Verify items look like cache paths
        cache_indicators = ['__pycache__', '.pyc', '.pyo', '.pytest_cache',
                           '.ruff_cache', '.mypy_cache', '.hypothesis',
                           '.tox', '.eggs', '.coverage']
        for item in removed_items:
            assert any(indicator in item for indicator in cache_indicators), \
                f"Item '{item}' doesn't look like a cache file/directory"

        # Display results with visual distinction
        print("\n")
        print("=" * 70)
        print("🧹 CACHE CLEANUP (Live Action on Project)")
        print("=" * 70)
        output = format_cache_statistics(removed_count, removed_items)
        print(output)
        print("=" * 70)
        print()
    else:
        # No caches found - this is fine (idempotent behavior)
        assert len(removed_items) == 0, \
            f"Count is 0 but list has {len(removed_items)} items"
        # Display results with visual distinction
        print("\n")
        print("=" * 70)
        print("🧹 CACHE CLEANUP (Live Action on Project)")
        print("=" * 70)
        output = format_cache_statistics(removed_count, removed_items)
        print(output)
        print("=" * 70)
        print()
