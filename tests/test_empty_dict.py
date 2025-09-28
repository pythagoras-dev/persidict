import pytest
from persidict import EmptyDict


def test_empty_dict_basic_operations():
    """Test that EmptyDict behaves like a null device for basic operations."""
    empty_dict = EmptyDict()
    
    # Test initial state
    assert len(empty_dict) == 0
    assert list(empty_dict) == []
    assert list(empty_dict.keys()) == []
    assert list(empty_dict.values()) == []
    assert list(empty_dict.items()) == []
    
    # Test write operations (should be discarded)
    empty_dict["key1"] = "value1"
    empty_dict[("complex", "key")] = {"complex": "value"}
    empty_dict["123"] = [1, 2, 3]
    
    # After writes, dict should still be empty
    assert len(empty_dict) == 0
    assert "key1" not in empty_dict
    assert ("complex", "key") not in empty_dict
    assert 123 not in empty_dict
    
    # Test read operations (should raise KeyError or return default)
    with pytest.raises(KeyError):
        _ = empty_dict["key1"]
    
    with pytest.raises(KeyError):
        _ = empty_dict[("complex", "key")]
    
    # Test get method (should return default)
    assert empty_dict.get("key1") is None
    assert empty_dict.get("key1", "default") == "default"
    assert empty_dict.get(123, []) == []
    
    # Test setdefault (should return default without storing)
    result = empty_dict.setdefault("new_key", "default_value")
    assert result == "default_value"
    assert "new_key" not in empty_dict
    assert len(empty_dict) == 0


def test_empty_dict_delete_operations():
    """Test delete operations on EmptyDict."""
    empty_dict = EmptyDict()
    
    # Try to delete non-existent key
    with pytest.raises(KeyError):
        del empty_dict["non_existent"]
    
    # Test discard (should return False)
    assert empty_dict.discard("any_key") == False
    assert empty_dict.discard(("complex", "key")) == False


def test_empty_dict_timestamp_operations():
    """Test timestamp-related operations on EmptyDict."""
    empty_dict = EmptyDict()
    
    # Timestamp should raise KeyError for any key
    with pytest.raises(KeyError):
        empty_dict.timestamp("any_key")
    
    # Oldest/newest operations should return empty
    assert list(empty_dict.oldest_keys()) == []
    assert list(empty_dict.oldest_values()) == []
    assert list(empty_dict.newest_keys()) == []
    assert list(empty_dict.newest_values()) == []


def test_empty_dict_iteration():
    """Test that all iteration methods return empty results."""
    empty_dict = EmptyDict()
    
    # Add some data (which should be discarded)
    empty_dict["key1"] = "value1"
    empty_dict["key2"] = "value2"
    
    # All iterations should still be empty
    assert list(empty_dict) == []
    assert list(empty_dict.keys()) == []
    assert list(empty_dict.values()) == []
    assert list(empty_dict.items()) == []
    assert list(empty_dict.keys_and_timestamps()) == []
    assert list(empty_dict.values_and_timestamps()) == []
    assert list(empty_dict.items_and_timestamps()) == []


def test_empty_dict_clear_operation():
    """Test clear operation (should be no-op)."""
    empty_dict = EmptyDict()
    
    # Clear should work without error
    empty_dict.clear()
    assert len(empty_dict) == 0
    
    # Clear after "adding" data should still result in empty dict
    empty_dict["key"] = "value"
    empty_dict.clear()
    assert len(empty_dict) == 0


def test_empty_dict_subdict():
    """Test subdict operations."""
    empty_dict = EmptyDict()
    
    # Get subdict should return another EmptyDict
    subdict = empty_dict.get_subdict(("prefix",))
    assert isinstance(subdict, EmptyDict)
    assert len(subdict) == 0
    
    # Subdicts should return empty
    assert list(empty_dict.subdicts()) == []


def test_empty_dict_properties():
    """Test EmptyDict properties."""
    empty_dict = EmptyDict()
    
    # Get params should work
    params = empty_dict.get_params()
    assert isinstance(params, dict)


def test_empty_dict_random_key():
    """Test random_key operation."""
    empty_dict = EmptyDict()
    empty_dict["asdr"] = 50
    
    # Should raise KeyError since there are no keys

    assert empty_dict.random_key() is None


def test_empty_dict_consistency():
    """Test that EmptyDict maintains consistency after multiple operations."""
    empty_dict = EmptyDict()
    
    # Perform many operations
    for i in range(100):
        empty_dict[f"key_{i}"] = f"value_{i}"
        empty_dict[(f"tuple_key_{i}", str(i))] = {"data": i}
    
    # Should still be empty
    assert len(empty_dict) == 0
    assert list(empty_dict) == []
    
    # Try to access any key
    for i in range(10):
        assert f"key_{i}" not in empty_dict
        assert empty_dict.get(f"key_{i}") is None
        assert empty_dict.discard(f"key_{i}") == False