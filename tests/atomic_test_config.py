"""Test configuration for atomic types tests.

Uses only LocalDict and FileDirDict with default parameters to keep
the test matrix small and focused on type serialization behavior.
"""

from persidict import LocalDict, FileDirDict
from tests.data_for_mutable_tests import make_test_dict

# Simplified test matrix: LocalDict (in-memory) and FileDirDict (disk-based)
# Each with default pickle serialization which works for all types
atomic_type_tests = [LocalDict, FileDirDict]
