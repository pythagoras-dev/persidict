"""Live-on-self CI/CD "test" for mf_get_stats() functionality.

This "test" runs against the actual project files (README.md and docs/source/index.rst)
to ensure that statistics are always up-to-date. It's a CI/CD action masquerading as
a regular test - it validates that the documentation can be updated with current stats
by actually updating the documentation files.

The test fails if:
1. README.md and/or docs/source/index.rst are not found
2. Required markers are missing from these files
3. mf_get_stats() fails to update them
"""
import pytest
from pathlib import Path

from mixinforge.command_line_tools.project_analyzer import analyze_project
from mixinforge.command_line_tools._cli_entry_points import (
    _update_readme_if_possible,
    _update_rst_docs_if_possible
)


@pytest.mark.live_actions
def test_live_stats_update(pytestconfig):
    """Test that mf_get_stats() can update actual project documentation files.

    This is a live-on-self test that validates:
    - README.md exists with proper markers
    - docs/source/index.rst exists with proper markers
    - analyze_project() generates valid stats
    - Both files can be successfully updated with current stats
    - Content is actually inserted between markers

    Args:
        pytestconfig: Pytest config fixture providing rootdir.
    """
    # Get project root from pytest's rootdir
    project_root = Path(pytestconfig.rootdir)

    # Validate README.md exists
    readme_path = project_root / "README.md"
    assert readme_path.exists(), f"README.md not found at {readme_path}"

    # Validate README.md has required markers
    readme_content = readme_path.read_text()
    assert '<!-- MIXINFORGE_STATS_START -->' in readme_content, \
        "README.md missing <!-- MIXINFORGE_STATS_START --> marker"
    assert '<!-- MIXINFORGE_STATS_END -->' in readme_content, \
        "README.md missing <!-- MIXINFORGE_STATS_END --> marker"

    # Validate docs/source/index.rst exists
    index_rst_path = project_root / "docs" / "source" / "index.rst"
    assert index_rst_path.exists(), f"index.rst not found at {index_rst_path}"

    # Validate index.rst has required markers
    index_rst_content = index_rst_path.read_text()
    assert '.. MIXINFORGE_STATS_START' in index_rst_content, \
        "index.rst missing .. MIXINFORGE_STATS_START marker"
    assert '.. MIXINFORGE_STATS_END' in index_rst_content, \
        "index.rst missing .. MIXINFORGE_STATS_END marker"

    # Generate fresh statistics
    analysis = analyze_project(project_root, verbose=False)
    markdown_content = analysis.to_markdown()
    rst_content = analysis.to_rst()

    # Verify markdown content is valid (should contain table structure)
    assert '|' in markdown_content, "Generated markdown should contain table structure"
    assert 'LOC' in markdown_content or 'Lines' in markdown_content, \
        "Generated markdown should contain LOC/Lines metric"

    # Verify RST content is valid (should contain list-table directive)
    assert '.. list-table::' in rst_content, "Generated RST should contain list-table directive"
    assert 'LOC' in rst_content or 'Lines' in rst_content, \
        "Generated RST should contain LOC/Lines metric"

    # Store original content to verify updates actually happen
    original_readme = readme_content
    original_index_rst = index_rst_content

    # Attempt to update README.md
    updated_readme_path = _update_readme_if_possible(project_root, markdown_content)

    # Function returns None if content didn't change (already up-to-date)
    # This is valid and expected - verify the content is there regardless
    if updated_readme_path is not None:
        assert updated_readme_path == readme_path, \
            f"Updated path mismatch: expected {readme_path}, got {updated_readme_path}"
        status_readme = "updated"
    else:
        status_readme = "already up-to-date"

    # Verify README.md has valid stats content (whether updated or already there)
    import re
    new_readme_content = readme_path.read_text()

    # Use regex to find markers on their own lines (same logic as the update function)
    start_pattern = r'^<!-- MIXINFORGE_STATS_START -->\s*$'
    end_pattern = r'^<!-- MIXINFORGE_STATS_END -->\s*$'
    start_matches = list(re.finditer(start_pattern, new_readme_content, re.MULTILINE))
    end_matches = list(re.finditer(end_pattern, new_readme_content, re.MULTILINE))

    assert len(start_matches) == 1, "README.md should have exactly one standalone START marker"
    assert len(end_matches) == 1, "README.md should have exactly one standalone END marker"

    start_idx = start_matches[0].end()
    end_idx = end_matches[0].start()
    readme_stats_section = new_readme_content[start_idx:end_idx].strip()

    assert len(readme_stats_section) > 0, "README.md stats section is empty"
    assert '|' in readme_stats_section, "README.md stats section should contain table"

    # Attempt to update index.rst
    updated_rst_path = _update_rst_docs_if_possible(project_root, rst_content)

    # Function returns None if content didn't change (already up-to-date)
    if updated_rst_path is not None:
        assert updated_rst_path == index_rst_path, \
            f"Updated path mismatch: expected {index_rst_path}, got {updated_rst_path}"
        status_rst = "updated"
    else:
        status_rst = "already up-to-date"

    # Verify index.rst has valid stats content (whether updated or already there)
    new_index_rst_content = index_rst_path.read_text()

    # Use regex to find markers on their own lines (same logic as the update function)
    start_pattern = r'^\.\. MIXINFORGE_STATS_START\s*$'
    end_pattern = r'^\.\. MIXINFORGE_STATS_END\s*$'
    start_matches = list(re.finditer(start_pattern, new_index_rst_content, re.MULTILINE))
    end_matches = list(re.finditer(end_pattern, new_index_rst_content, re.MULTILINE))

    assert len(start_matches) == 1, "index.rst should have exactly one standalone START marker"
    assert len(end_matches) == 1, "index.rst should have exactly one standalone END marker"

    start_idx = start_matches[0].end()
    end_idx = end_matches[0].start()
    rst_stats_section = new_index_rst_content[start_idx:end_idx].strip()

    assert len(rst_stats_section) > 0, "index.rst stats section is empty"
    assert '.. list-table::' in rst_stats_section, \
        "index.rst stats section should contain list-table directive"

    # Display results with visual distinction
    print("\n")
    print("=" * 70)
    print("📊 STATS UPDATE (Live Action on Project)")
    print("=" * 70)
    print(f"✓ Validation successful:")
    print(f"  • README.md: {status_readme}")
    print(f"  • index.rst: {status_rst}")
    print("=" * 70)
    print()
