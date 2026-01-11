"""Version information for the mixinforge package."""

from importlib import metadata as _md

try:
    __version__ = _md.version("persidict")
except _md.PackageNotFoundError:
    __version__ = "unknown"
