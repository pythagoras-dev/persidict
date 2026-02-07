from __future__ import annotations

from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent

INTEGRATION_DIRS = (
    TESTS_DIR / "simple_storage_service",
    TESTS_DIR / "compatibility_serialization",
    TESTS_DIR / "entity_tag_operations",
)

SLOW_DIRS = (
    TESTS_DIR / "atomic_type_support",
    TESTS_DIR / "timestamp_behavior",
)

SLOW_FILES = (
    TESTS_DIR / "storage_backends" / "test_concurrency_filedirdict.py",
)

_FILE_MARKER_CACHE: dict[Path, dict[str, bool]] = {}


def _is_in_dir(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        path = Path(str(item.fspath)).resolve()
        file_flags = _FILE_MARKER_CACHE.get(path)
        if file_flags is None:
            try:
                contents = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                contents = ""
            file_flags = {
                "integration": ("mock_aws" in contents) or ("mutable_tests" in contents),
            }
            _FILE_MARKER_CACHE[path] = file_flags

        if any(_is_in_dir(path, directory) for directory in INTEGRATION_DIRS):
            item.add_marker(pytest.mark.integration)
        elif file_flags.get("integration"):
            item.add_marker(pytest.mark.integration)

        if any(_is_in_dir(path, directory) for directory in SLOW_DIRS):
            item.add_marker(pytest.mark.slow)

        if path in SLOW_FILES:
            item.add_marker(pytest.mark.slow)
