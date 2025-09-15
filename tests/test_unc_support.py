#!/usr/bin/env python3
"""Test script to verify UNC path support in FileDirDict."""

import os
import tempfile
import sys
import shutil

# Add src to path to import persidict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from persidict.file_dir_dict import add_long_path_prefix, drop_long_path_prefix


def test_unc_path_functions():
    """Test UNC path handling functions on all platforms."""
    print("Testing UNC path handling functions...")
    
    # Test regular paths
    regular_path = r"C:\temp\test"
    if os.name == 'nt':
        expected_regular = r"\\?\C:\temp\test"
        assert add_long_path_prefix(regular_path) == expected_regular
        assert drop_long_path_prefix(expected_regular) == regular_path
        print(f"✓ Regular path: {regular_path} -> {add_long_path_prefix(regular_path)}")
    else:
        # On non-Windows, functions should return path unchanged
        assert add_long_path_prefix(regular_path) == regular_path
        assert drop_long_path_prefix(regular_path) == regular_path
        print(f"✓ Regular path (non-Windows): {regular_path} -> {add_long_path_prefix(regular_path)}")
    
    # Test UNC paths
    unc_path = r"\\server\share\folder\file.txt"
    if os.name == 'nt':
        expected_unc = r"\\?\UNC\server\share\folder\file.txt"
        result = add_long_path_prefix(unc_path)
        assert result == expected_unc, f"Expected {expected_unc}, got {result}"
        assert drop_long_path_prefix(expected_unc) == unc_path
        print(f"✓ UNC path: {unc_path} -> {result}")
        
        # Test reverse conversion
        reverse_result = drop_long_path_prefix(expected_unc)
        assert reverse_result == unc_path, f"Expected {unc_path}, got {reverse_result}"
        print(f"✓ UNC path reverse: {expected_unc} -> {reverse_result}")
    else:
        # On non-Windows, functions should return path unchanged
        assert add_long_path_prefix(unc_path) == unc_path
        assert drop_long_path_prefix(unc_path) == unc_path
        print(f"✓ UNC path (non-Windows): {unc_path} -> {add_long_path_prefix(unc_path)}")
    
    # Test already prefixed paths (should not double-prefix)
    already_prefixed = r"\\?\C:\temp\test"
    assert add_long_path_prefix(already_prefixed) == already_prefixed
    print(f"✓ Already prefixed path: {already_prefixed} -> {add_long_path_prefix(already_prefixed)}")
    
    already_prefixed_unc = r"\\?\UNC\server\share\test"
    assert add_long_path_prefix(already_prefixed_unc) == already_prefixed_unc
    print(f"✓ Already prefixed UNC: {already_prefixed_unc} -> {add_long_path_prefix(already_prefixed_unc)}")


def test_filedirdict_with_unc_base_dir():
    """Test FileDirDict with UNC-like base directory (simulated)."""
    from persidict.file_dir_dict import FileDirDict
    
    print("\nTesting FileDirDict with UNC-like base directory...")
    
    # Create a temporary directory that simulates UNC path structure
    with tempfile.TemporaryDirectory() as temp_dir:
        # On Windows, we can't easily test real UNC paths without a network share,
        # but we can test the path handling logic
        if os.name == 'nt':
            # Simulate UNC-like path (though it's actually local)
            unc_like_base = os.path.join(temp_dir, "server", "share", "test_dict")
        else:
            # On non-Windows, just use a regular path
            unc_like_base = os.path.join(temp_dir, "test_dict")
        
        try:
            # Create FileDirDict with UNC-like base directory
            fdd = FileDirDict(base_dir=unc_like_base)
            
            # Test basic operations
            fdd["test_key"] = "test_value"
            assert fdd["test_key"] == "test_value"
            assert "test_key" in fdd
            
            # Test nested keys
            fdd["dir", "subdir", "file"] = {"data": "nested_value"}
            assert fdd["dir", "subdir", "file"]["data"] == "nested_value"
            
            print("✓ FileDirDict operations work with UNC-like base directory")
            
        except Exception as e:
            print(f"✗ Error with UNC-like base directory: {e}")
            raise