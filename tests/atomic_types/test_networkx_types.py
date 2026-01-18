"""Tests for storing and retrieving networkx atomic types in PersiDict."""

import pytest
import networkx as nx

from ..atomic_test_config import atomic_type_tests


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_networkx_graph(tmp_path, DictToTest):
    """Verify networkx Graph values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = nx.Graph()
    original.add_edges_from([(1, 2), (2, 3), (3, 1)])
    d["key"] = original
    retrieved = d["key"]
    assert set(retrieved.nodes()) == set(original.nodes())
    assert set(retrieved.edges()) == set(original.edges())

    d.clear()


@pytest.mark.parametrize("DictToTest", atomic_type_tests)
def test_networkx_digraph(tmp_path, DictToTest):
    """Verify networkx DiGraph values can be stored and retrieved."""
    d = DictToTest(base_dir=tmp_path)
    d.clear()

    original = nx.DiGraph()
    original.add_edges_from([(1, 2), (2, 3), (3, 1)])
    d["key"] = original
    retrieved = d["key"]
    assert set(retrieved.nodes()) == set(original.nodes())
    assert set(retrieved.edges()) == set(original.edges())

    d.clear()
