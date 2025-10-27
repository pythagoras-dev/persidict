from moto import mock_aws
from persidict import FileDirDict, S3Dict
from persidict.safe_str_tuple import SafeStrTuple
from minimum_sleep import min_sleep


def to_str_list(safe_tuple_list):
    """Convert list of SafeStrTuple objects to list of strings."""
    result = []
    for item in safe_tuple_list:
        if isinstance(item, SafeStrTuple):
            result.append(item[0] if len(item) > 0 else str(item))
        else:
            result.append(item)
    return result


def test_file_dir_dict_timestamp_functions(tmpdir):
    """Test timestamp functions specifically for FileDirDict with proper timing."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add items with delays
    for i, v in enumerate("abcde"):
        d[v] = f"value_{v}"
        min_sleep(d)  # Sufficient delay for FileDirDict
    
    # Test oldest_keys
    oldest_keys = d.oldest_keys(3)
    assert len(oldest_keys) == 3
    assert to_str_list(oldest_keys) == ['a', 'b', 'c']
    
    # Test newest_keys
    newest_keys = d.newest_keys(3)
    assert len(newest_keys) == 3
    assert to_str_list(newest_keys) == ['e', 'd', 'c']
    
    # Test oldest_values
    oldest_values = d.oldest_values(3)
    assert len(oldest_values) == 3
    assert oldest_values == ['value_a', 'value_b', 'value_c']
    
    # Test newest_values
    newest_values = d.newest_values(3)
    assert len(newest_values) == 3
    assert newest_values == ['value_e', 'value_d', 'value_c']
    
    # Test reverse relationship
    assert to_str_list(d.newest_keys()) == list(reversed(to_str_list(d.oldest_keys())))
    assert d.newest_values() == list(reversed(d.oldest_values()))

    d.clear()


@mock_aws
def test_s3_dict_timestamp_functions(tmpdir):
    """Test timestamp functions with longer delays."""
    d = S3Dict(base_dir=tmpdir.mkdir("AWS"), bucket_name="mybucket")
    
    # Add items with longer delays timestamp precision
    for i, v in enumerate("abc"):  # Use fewer items to reduce test time
        d[v] = f"value_{v}"
        min_sleep(d)  # Longer delay
    
    # Test basic functionality
    oldest_keys = d.oldest_keys()
    newest_keys = d.newest_keys()
    oldest_values = d.oldest_values()
    newest_values = d.newest_values()
    
    # Verify lengths
    assert len(oldest_keys) == 3
    assert len(newest_keys) == 3
    assert len(oldest_values) == 3
    assert len(newest_values) == 3
    
    # Test that functions return appropriate types
    assert all(isinstance(k, SafeStrTuple) for k in oldest_keys)
    assert all(isinstance(k, SafeStrTuple) for k in newest_keys)
    assert all(isinstance(v, str) for v in oldest_values)
    assert all(isinstance(v, str) for v in newest_values)

    d.clear()


def test_empty_dict_edge_cases(tmpdir):
    """Test edge cases with empty dictionaries for both implementations."""
    for d in [
        FileDirDict(base_dir=tmpdir.mkdir("LOCAL")),
    ]:
        # Test all functions on empty dict
        assert d.oldest_keys() == []
        assert d.newest_keys() == []
        assert d.oldest_values() == []
        assert d.newest_values() == []
        
        # Test with specific max_n values
        assert d.oldest_keys(5) == []
        assert d.newest_keys(5) == []
        assert d.oldest_values(5) == []
        assert d.newest_values(5) == []
        
        # Test with max_n=0
        assert d.oldest_keys(0) == []
        assert d.newest_keys(0) == []
        assert d.oldest_values(0) == []
        assert d.newest_values(0) == []

        d.clear()


def test_single_item_edge_cases(tmpdir):
    """Test edge cases with single item in dictionary."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    d['single'] = 'value'
    
    # Test all functions with single item
    assert len(d.oldest_keys()) == 1
    assert len(d.newest_keys()) == 1
    assert d.oldest_values() == ['value']
    assert d.newest_values() == ['value']
    
    # Test with max_n=1
    assert len(d.oldest_keys(1)) == 1
    assert len(d.newest_keys(1)) == 1
    assert d.oldest_values(1) == ['value']
    assert d.newest_values(1) == ['value']
    
    # Test with max_n larger than available
    assert len(d.oldest_keys(5)) == 1
    assert len(d.newest_keys(5)) == 1
    assert d.oldest_values(5) == ['value']
    assert d.newest_values(5) == ['value']

    d.clear()


def test_max_n_zero_edge_cases(tmpdir):
    """Test edge cases with max_n=0."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add some items
    for v in "abc":
        d[v] = f"value_{v}"
        min_sleep(d)
    
    # Test all functions with max_n=0
    assert d.oldest_keys(0) == []
    assert d.newest_keys(0) == []
    assert d.oldest_values(0) == []
    assert d.newest_values(0) == []

    d.clear()


def test_ordering_after_deletion(tmpdir):
    """Test that ordering is maintained correctly after deletions."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add items with delays
    for v in "abcdefg":
        d[v] = f"value_{v}"
        min_sleep(d)
    
    # Delete some middle items
    del d['c']
    del d['e']
    
    # Verify oldest ordering after deletion
    oldest_keys = d.oldest_keys()
    assert to_str_list(oldest_keys) == ['a', 'b', 'd', 'f', 'g']
    oldest_values = d.oldest_values()
    assert oldest_values == ['value_a', 'value_b', 'value_d', 'value_f', 'value_g']
    
    # Verify newest ordering after deletion
    newest_keys = d.newest_keys()
    assert to_str_list(newest_keys) == ['g', 'f', 'd', 'b', 'a']
    newest_values = d.newest_values()
    assert newest_values == ['value_g', 'value_f', 'value_d', 'value_b', 'value_a']
    
    # Test with limited max_n after deletion
    assert to_str_list(d.oldest_keys(3)) == ['a', 'b', 'd']
    assert to_str_list(d.newest_keys(3)) == ['g', 'f', 'd']
    assert d.oldest_values(3) == ['value_a', 'value_b', 'value_d']
    assert d.newest_values(3) == ['value_g', 'value_f', 'value_d']

    d.clear()


def test_timestamp_verification(tmpdir):
    """Test that functions actually return items in timestamp order."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add items with delays and store timestamps
    timestamps = {}
    for v in "abcde":
        d[v] = f"value_{v}"
        min_sleep(d)
        key_obj = SafeStrTuple((v,))
        timestamps[key_obj] = d.timestamp(v)
        timestamps[v] = d.timestamp(v)
    
    # Verify oldest_keys returns keys in timestamp order
    oldest_keys = d.oldest_keys()
    for i in range(len(oldest_keys) - 1):
        current_key = oldest_keys[i]
        next_key = oldest_keys[i + 1]
        assert timestamps[current_key] <= timestamps[next_key], \
            f"Timestamp order violated: {current_key} should be older than {next_key}"
    
    # Verify newest_keys returns keys in reverse timestamp order
    newest_keys = d.newest_keys()
    for i in range(len(newest_keys) - 1):
        current_key = newest_keys[i]
        next_key = newest_keys[i + 1]
        assert timestamps[current_key] >= timestamps[next_key], \
            f"Reverse timestamp order violated: {current_key} should be newer than {next_key}"
    
    # Verify values functions return values for keys in correct order
    oldest_values = d.oldest_values()
    expected_oldest_values = [d[k] for k in oldest_keys]
    assert oldest_values == expected_oldest_values
    
    newest_values = d.newest_values()
    expected_newest_values = [d[k] for k in newest_keys]
    assert newest_values == expected_newest_values

    d.clear()


def test_consistency_between_functions(tmpdir):
    """Test consistency between keys and values functions."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add items
    for v in "abcdef":
        d[v] = f"value_{v}"
        min_sleep(d)
    
    # Test consistency for various max_n values
    for max_n in [None, 1, 3, 5, 10]:
        oldest_keys = d.oldest_keys(max_n)
        oldest_values = d.oldest_values(max_n)
        newest_keys = d.newest_keys(max_n)
        newest_values = d.newest_values(max_n)
        
        # Values should match keys
        assert oldest_values == [d[k] for k in oldest_keys]
        assert newest_values == [d[k] for k in newest_keys]
        
        # Length consistency
        assert len(oldest_keys) == len(oldest_values)
        assert len(newest_keys) == len(newest_values)
    
    # Test the relationship
    assert to_str_list(d.newest_keys(100)) == list(reversed(to_str_list(d.oldest_keys(100))))
    assert d.newest_values(100) == list(reversed(d.oldest_values(100)))

    d.clear()


def test_different_data_types(tmpdir):
    """Test functions with different value data types."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Add items with different value types
    test_values = [
        ('str_key', 'string_value'),
        ('int_key', 42),
        ('list_key', [1, 2, 3]),
        ('dict_key', {'nested': 'dict'}),
        ('bool_key', True)
    ]
    
    for key, value in test_values:
        d[key] = value
        min_sleep(d)
    
    # Test that all functions work with different data types
    oldest_keys = d.oldest_keys()
    newest_keys = d.newest_keys()
    oldest_values = d.oldest_values()
    newest_values = d.newest_values()
    
    assert len(oldest_keys) == len(test_values)
    assert len(newest_keys) == len(test_values)
    assert len(oldest_values) == len(test_values)
    assert len(newest_values) == len(test_values)
    
    # Verify values are retrieved correctly
    expected_values = [v[1] for v in test_values]
    for key in oldest_keys:
        assert d[key] in expected_values
    
    for key in newest_keys:
        assert d[key] in expected_values

    d.clear()


def test_multiple_operations_and_updates(tmpdir):
    """Test behavior after multiple operations including updates."""
    d = FileDirDict(base_dir=tmpdir.mkdir("LOCAL"))
    
    # Initial setup
    d['a'] = 'value_a_1'
    min_sleep(d)
    d['b'] = 'value_b_1'
    min_sleep(d)
    d['c'] = 'value_c_1'
    min_sleep(d)
    
    # Update an existing key (should change its timestamp)
    d['a'] = 'value_a_2'
    min_sleep(d)
    
    # Add new key
    d['d'] = 'value_d_1'
    
    # Now 'a' should be newer than 'b' and 'c' but older than 'd'
    oldest_keys = d.oldest_keys()
    newest_keys = d.newest_keys()
    
    # Check that 'b' and 'c' are oldest (in that order)
    assert oldest_keys[0][0] == 'b'
    assert oldest_keys[1][0] == 'c'
    
    # Check that 'd' is newest, 'a' is second newest
    assert newest_keys[0][0] == 'd'
    assert newest_keys[1][0] == 'a'
    
    # Test values consistency
    oldest_values = d.oldest_values()
    newest_values = d.newest_values()
    
    assert oldest_values == [d[k] for k in oldest_keys]
    assert newest_values == [d[k] for k in newest_keys]
    
    # Verify the updated value is returned
    assert 'value_a_2' in oldest_values or 'value_a_2' in newest_values
    assert 'value_a_1' not in oldest_values and 'value_a_1' not in newest_values

    d.clear()