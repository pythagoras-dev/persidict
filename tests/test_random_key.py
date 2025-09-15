import pytest
from moto import mock_aws
from collections import Counter
import random
import time
from persidict import SafeStrTuple

from data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_empty_dict_returns_none(tmpdir, DictToTest, kwargs):
    """Test that random_key returns None for an empty dictionary."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    assert dict_to_test.random_key() is None


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_single_item_dict(tmpdir, DictToTest, kwargs):
    """Test that random_key returns the only key for a single-item dictionary."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test["single_key"] = "single_value"
    assert dict_to_test.random_key() == "single_key"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_multi_item_dict_with_simple_keys(tmpdir, DictToTest, kwargs):
    """Test that random_key returns a valid key for a multi-item dictionary with simple keys."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    N=50

    # Add multiple items
    for n in range(N):
        dict_to_test[str(n)] = n**2

    all_found_keys = set()

    # Check that random_key returns a valid key
    for _ in range(N*2):
        random_key = dict_to_test.random_key()
        assert random_key is not None
        assert random_key in dict_to_test
        all_found_keys.add(random_key)

    assert len(all_found_keys) > n/2


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_complex_keys(tmpdir, DictToTest, kwargs):
    """Test that random_key works correctly with complex keys (tuples of strings)."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    complex_keys = [
        ("a", "1"), 
        ("b", "2"), 
        ("c", "3")
    ]

    # Store values with a simple pattern we can verify
    for key in complex_keys:
        dict_to_test[key] = f"value-{key[0]}-{key[1]}"

    # Verify that random_key returns a valid key and we can access the value
    for _ in range(5):
        random_key = dict_to_test.random_key()
        assert random_key is not None

        # The key returned by random_key() might be a SafeStrTuple, not the original tuple
        # We need to check that it corresponds to one of our original keys
        value = dict_to_test[random_key]
        assert value.startswith("value-")

        # Extract the components from the value to verify it matches one of our keys
        parts = value.split("-")
        assert len(parts) == 3
        assert (parts[1], parts[2]) in complex_keys


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_randomness_distribution(tmpdir, DictToTest, kwargs):
    """Test that random_key provides a uniform distribution of keys."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add 5 keys to the dictionary
    for i in range(5):
        dict_to_test[str(i)] = i

    # Sample random keys many times to check distribution
    samples = [dict_to_test.random_key() for _ in range(500)]
    counter = Counter(samples)

    # Check that all keys are present in the samples
    assert set(counter.keys()) == set(dict_to_test.keys())

    # Check that the distribution is roughly uniform
    # Each key should appear approximately 100 times (500/5)
    # Allow for some statistical variation
    for key, count in counter.items():
        assert 60 <= count <= 140, f"Key {key} appeared {count} times, expected around 100"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_after_removing_keys(tmpdir, DictToTest, kwargs):
    """Test that random_key only returns remaining keys after some keys are removed."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add keys
    for i in range(10):
        dict_to_test[str(i)] = i

    # Remove some keys
    keys_to_remove = [str(i) for i in range(0, 10, 2)]  # Remove even numbered keys
    for key in keys_to_remove:
        del dict_to_test[key]

    # Check that random_key only returns remaining keys
    for _ in range(20):
        random_key = dict_to_test.random_key()
        assert random_key not in keys_to_remove
        assert random_key in dict_to_test


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_after_adding_keys(tmpdir, DictToTest, kwargs):
    """Test that random_key includes newly added keys."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Start with some keys
    initial_keys = [SafeStrTuple("key1"), SafeStrTuple("key2"), SafeStrTuple("key3")]
    for key in initial_keys:
        dict_to_test[key] = f"value-{key}"

    # Add new keys
    new_keys = [SafeStrTuple("new1"), SafeStrTuple("new2"), SafeStrTuple("new3")]
    for key in new_keys:
        dict_to_test[key] = f"value-{key}"

    # Sample many times to ensure new keys are included
    all_keys = set(initial_keys + new_keys)
    samples = [dict_to_test.random_key() for _ in range(100)]

    # Check that both initial and new keys appear in samples
    sampled_keys = set(samples)
    assert sampled_keys == all_keys


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_consistency_with_keys_method(tmpdir, DictToTest, kwargs):
    """Test that random_key only returns keys that would be returned by the keys() method."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add various keys
    dict_to_test["str_key"] = "string value"
    dict_to_test[("tuple", "key")] = "tuple value"
    dict_to_test["numeric_key"] = 123

    # Get all keys
    all_keys = set(dict_to_test.keys())

    # Check that random_key returns one of these keys
    for _ in range(20):
        random_key = dict_to_test.random_key()
        assert random_key in all_keys


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_with_many_keys(tmpdir, DictToTest, kwargs):
    """Test random_key with a dictionary containing many keys."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add many keys (100 keys)
    for i in range(100):
        dict_to_test[f"key{i}"] = f"value{i}"

    # Check that random_key returns valid keys
    for _ in range(20):
        random_key = dict_to_test.random_key()
        assert random_key in dict_to_test
        assert random_key.strings[0].startswith("key")

    # Verify we can get different keys (not always the same one)
    samples = [dict_to_test.random_key() for _ in range(50)]
    assert len(set(samples)) > 1, "random_key should return different keys across multiple calls"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_clear_and_repopulate(tmpdir, DictToTest, kwargs):
    """Test random_key after clearing and repopulating the dictionary."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add initial keys
    for i in range(5):
        dict_to_test[f"initial{i}"] = i

    # Verify random_key works
    assert dict_to_test.random_key() is not None

    # Clear the dictionary
    dict_to_test.clear()
    assert dict_to_test.random_key() is None

    # Repopulate with new keys
    for i in range(5):
        dict_to_test[f"new{i}"] = i

    # Verify random_key returns one of the new keys
    random_key = dict_to_test.random_key()
    assert random_key is not None
    assert random_key.strings[0].startswith("new")
    assert random_key in dict_to_test


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_updating_keys(tmpdir, DictToTest, kwargs):
    """Test random_key after updating values for existing keys."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add initial keys and values
    for i in range(5):
        dict_to_test[f"key{i}"] = f"initial_value{i}"

    # Update values for existing keys
    for i in range(5):
        dict_to_test[f"key{i}"] = f"updated_value{i}"

    # Check that random_key still returns valid keys
    for _ in range(10):
        random_key = dict_to_test.random_key()
        assert random_key in dict_to_test

        # Verify the value has been updated
        value = dict_to_test[random_key]
        assert value.startswith("updated_value")


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_performance_with_large_dict(tmpdir, DictToTest, kwargs):
    """Test performance of random_key with a large dictionary."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Skip this test for slow implementations
    if hasattr(DictToTest, "slow_implementation") and DictToTest.slow_implementation:
        pytest.skip("Skipping performance test for slow implementation")

    # Add a moderate number of keys (adjust based on implementation)
    num_keys = 1000
    for i in range(num_keys):
        dict_to_test[f"key{i}"] = f"value{i}"

    # Measure time to call random_key multiple times
    start_time = time.time()
    num_calls = 100
    for _ in range(num_calls):
        random_key = dict_to_test.random_key()
        assert random_key in dict_to_test
    end_time = time.time()

    # Calculate average time per call
    avg_time = (end_time - start_time) / num_calls

    # This is a soft assertion - we're just logging the performance
    # but not failing the test based on it
    print(f"Average time for random_key() with {num_keys} keys: {avg_time:.6f} seconds")

    # Ensure the function completes in a reasonable time
    # This threshold might need adjustment based on the implementation
    assert avg_time < 1.0, f"random_key() took too long: {avg_time:.6f} seconds per call"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_exactly_two_items(tmpdir, DictToTest, kwargs):
    """Test random_key with a dictionary containing exactly two items."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add exactly two items
    dict_to_test["key1"] = "value1"
    dict_to_test["key2"] = "value2"

    # Sample many times to ensure both keys are returned
    samples = [dict_to_test.random_key() for _ in range(200)]
    unique_samples = set([s.strings[0] for s in samples])

    # Check that both keys are returned
    assert len(unique_samples) == 2
    assert "key1" in unique_samples
    assert "key2" in unique_samples

    # Check the distribution is roughly even (50/50)
    count_key1 = samples.count("key1")
    count_key2 = samples.count("key2")

    # Allow for some statistical variation (±50%)
    assert 50 <= count_key1 <= 150, f"Key 'key1' appeared {count_key1} times, expected around 100"
    assert 50 <= count_key2 <= 150, f"Key 'key2' appeared {count_key2} times, expected around 100"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_add_remove_same_key(tmpdir, DictToTest, kwargs):
    """Test random_key when repeatedly adding and removing the same key."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Start with some keys
    dict_to_test["permanent_key1"] = "value1"
    dict_to_test["permanent_key2"] = "value2"

    # Add and remove the same key multiple times
    for i in range(10):
        # Add the temporary key
        dict_to_test["temp_key"] = f"temp_value_{i}"

        # Check that random_key returns a valid key
        random_key = dict_to_test.random_key()
        assert random_key in dict_to_test

        # Remove the temporary key
        del dict_to_test["temp_key"]

        # Check that random_key still works and doesn't return the removed key
        random_key = dict_to_test.random_key()
        assert random_key in dict_to_test
        assert random_key != "temp_key"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_consistency_across_calls(tmpdir, DictToTest, kwargs):
    """Test that random_key is consistent in its behavior across multiple calls."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Add a fixed set of keys
    for i in range(10):
        dict_to_test[f"key{i}"] = f"value{i}"

    # Set a fixed seed for reproducibility
    random.seed(42)

    # Get a sequence of random keys
    first_sequence = [dict_to_test.random_key() for _ in range(20)]

    # Reset the seed and get another sequence
    random.seed(42)
    second_sequence = [dict_to_test.random_key() for _ in range(20)]

    # The sequences should be identical if random_key uses random.random() consistently
    # Note: This test assumes that random_key uses Python's random module and doesn't
    # have its own internal state or use other sources of randomness
    assert first_sequence == second_sequence, "random_key should be deterministic with a fixed random seed"


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_empty_then_add_then_empty(tmpdir, DictToTest, kwargs):
    """Test random_key behavior when alternating between empty and non-empty states."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)

    # Start with empty dictionary
    assert dict_to_test.random_key() is None

    # Add a key
    dict_to_test["key1"] = "value1"
    assert dict_to_test.random_key() == "key1"

    # Empty the dictionary
    dict_to_test.clear()
    assert dict_to_test.random_key() is None

    # Add multiple keys
    for i in range(5):
        dict_to_test[f"key{i}"] = f"value{i}"

    # Check that random_key returns a valid key
    assert dict_to_test.random_key() in dict_to_test

    # Empty the dictionary again
    dict_to_test.clear()
    assert dict_to_test.random_key() is None
