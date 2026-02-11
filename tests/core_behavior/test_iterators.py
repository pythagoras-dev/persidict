import pytest
from moto import mock_aws

from persidict import SafeStrTuple, EmptyDict
from tests.data_for_mutable_tests import mutable_tests


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_iterators(tmpdir, DictToTest, kwargs):
    """Test if iterators work correctly."""
    dict_to_test = DictToTest(base_dir=tmpdir, **kwargs)
    dict_to_test.clear()
    model_dict = dict()
    assert len(dict_to_test) == len(model_dict) == 0

    for i in range(25):
        k = f"key_{i*i}"
        dict_to_test[k] = 2*i
        model_dict[k] = 2*i

    assert (len(model_dict)
            == len(list(dict_to_test.keys()))
            == len(list(dict_to_test.values()))
            == len(list(dict_to_test.items())))

    assert sorted([str(k[0]) for k in dict_to_test.keys()]) == sorted(
        [str(k) for k in model_dict.keys()]) ##?!?!?!?!?!?!?
    assert sorted([str(v) for v in dict_to_test.values()]) == sorted(
        [str(v) for v in model_dict.values()])

    dict_to_test.clear()


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_generic_iter_all_result_types(tmpdir, DictToTest, kwargs):
    """Every valid result_type subset yields correct item count and shape.

    Covers the singleton {"timestamps"} case (no public method exposes it)
    alongside all other combinations, verifying tuple structure for each.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()
    n_items = 3
    for i in range(n_items):
        d[f"k{i}"] = i * 10

    # Single-element result types yield bare values, not 1-tuples.
    for result_type, expected_type in [
        ({"keys"}, SafeStrTuple),
        ({"values"}, int),
        ({"timestamps"}, float),
    ]:
        results = list(d._generic_iter(result_type))
        assert len(results) == n_items
        assert all(isinstance(r, expected_type) for r in results)

    # Multi-element result types yield tuples.
    multi_specs = [
        ({"keys", "values"}, (SafeStrTuple, int)),
        ({"keys", "timestamps"}, (SafeStrTuple, float)),
        ({"values", "timestamps"}, (int, float)),
        ({"keys", "values", "timestamps"}, (SafeStrTuple, int, float)),
    ]
    for result_type, type_pattern in multi_specs:
        results = list(d._generic_iter(result_type))
        assert len(results) == n_items
        for row in results:
            assert isinstance(row, tuple)
            assert len(row) == len(type_pattern)
            for element, expected in zip(row, type_pattern):
                assert isinstance(element, expected)

    d.clear()


def test_generic_iter_timestamps_only_empty_dict():
    """EmptyDict returns no items for {"timestamps"} without error."""
    d = EmptyDict()
    assert list(d._generic_iter({"timestamps"})) == []


def test_assemble_iter_result_field_ordering():
    """_assemble_iter_result always emits fields in (key, value, timestamp) order.

    Single-element requests return a bare value; multi-element requests
    return a tuple whose positions follow the canonical ordering regardless
    of the insertion order of the result_type set.
    """
    d = EmptyDict()
    key, value, ts = SafeStrTuple(("a",)), 42, 1.0

    # Single-element requests yield the bare value.
    assert d._assemble_iter_result({"keys"}, key=key, value=value, timestamp=ts) == key
    assert d._assemble_iter_result({"values"}, key=key, value=value, timestamp=ts) == value
    assert d._assemble_iter_result({"timestamps"}, key=key, value=value, timestamp=ts) == ts

    # Two-element requests yield a 2-tuple in canonical order.
    assert d._assemble_iter_result({"keys", "values"}, key=key, value=value, timestamp=ts) == (key, value)
    assert d._assemble_iter_result({"keys", "timestamps"}, key=key, value=value, timestamp=ts) == (key, ts)
    assert d._assemble_iter_result({"values", "timestamps"}, key=key, value=value, timestamp=ts) == (value, ts)

    # Three-element request yields the full triple.
    assert d._assemble_iter_result(
        {"keys", "values", "timestamps"}, key=key, value=value, timestamp=ts) == (key, value, ts)


@pytest.mark.parametrize("DictToTest, kwargs", mutable_tests)
@mock_aws
def test_generic_iter_field_values_consistent_across_result_types(tmpdir, DictToTest, kwargs):
    """Values from _generic_iter are consistent across different result_type combos.

    Writing known data and then iterating with different result_type sets
    must produce matching keys, values, and timestamps regardless of which
    fields are requested together.
    """
    d = DictToTest(base_dir=tmpdir, **kwargs)
    d.clear()
    expected = {"a": 10, "b": 20, "c": 30}
    for k, v in expected.items():
        d[k] = v

    full = {row[0]: row for row in d._generic_iter({"keys", "values", "timestamps"})}

    # keys-only iteration must match keys from the full triple.
    keys_only = set(d._generic_iter({"keys"}))
    assert keys_only == set(full.keys())

    # values-only iteration must match values from the full triple (as sorted lists).
    values_only = sorted(d._generic_iter({"values"}))
    values_from_full = sorted(row[1] for row in full.values())
    assert values_only == values_from_full

    # (key, value) pairs must match the first two elements of each full triple.
    kv_pairs = {kv[0]: kv[1] for kv in d._generic_iter({"keys", "values"})}
    for k, triple in full.items():
        assert kv_pairs[k] == triple[1]

    d.clear()
