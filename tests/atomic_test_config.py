"""Test configuration for atomic types tests.

Uses only LocalDict and FileDirDict with default parameters to keep
the test matrix small and focused on type serialization behavior.
"""

import inspect

from persidict import LocalDict, FileDirDict

# Simplified test matrix: LocalDict (in-memory) and FileDirDict (disk-based)
# Each with default pickle serialization which works for all types
atomic_type_tests = [LocalDict, FileDirDict]


def make_test_dict(dict_class, tmp_path=None, **kwargs):
    """Create a dict instance, filtering kwargs to only those accepted.

    Inspects the constructor of ``dict_class`` and passes only the keyword
    arguments it declares. ``base_dir`` is injected automatically for
    classes whose constructor accepts it (e.g. FileDirDict) when
    ``tmp_path`` is provided.
    """
    sig = inspect.signature(dict_class.__init__)
    accepted = set(sig.parameters.keys()) - {"self"}
    filtered = {k: v for k, v in kwargs.items() if k in accepted}
    if "base_dir" in accepted and tmp_path is not None:
        filtered.setdefault("base_dir", str(tmp_path))
    return dict_class(**filtered)
