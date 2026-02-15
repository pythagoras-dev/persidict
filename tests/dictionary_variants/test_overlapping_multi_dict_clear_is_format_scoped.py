"""Tests that OverlappingMultiDict.clear() is scoped to a single serialization format.

Clearing one sub-dict (e.g., json) must not affect data stored in other
sub-dicts (e.g., pkl) that share the same base directory.
"""

import pytest

from persidict import FileDirDict, LocalDict, OverlappingMultiDict


def test_clear_json_preserves_pkl(tmp_path):
    """Clearing the json sub-dict leaves pkl data intact."""
    omd = OverlappingMultiDict(
        dict_type=FileDirDict,
        shared_subdicts_params=dict(base_dir=str(tmp_path)),
        pkl={},
        json={},
    )

    omd.pkl["shared_key"] = "pkl_value"
    omd.json["shared_key"] = "json_value"
    omd.pkl["pkl_only"] = "only_in_pkl"

    omd.json.clear()

    assert len(omd.json) == 0
    assert omd.pkl["shared_key"] == "pkl_value"
    assert omd.pkl["pkl_only"] == "only_in_pkl"
    assert len(omd.pkl) == 2


def test_clear_pkl_preserves_json(tmp_path):
    """Clearing the pkl sub-dict leaves json data intact."""
    omd = OverlappingMultiDict(
        dict_type=FileDirDict,
        shared_subdicts_params=dict(base_dir=str(tmp_path)),
        pkl={},
        json={},
    )

    omd.pkl["k"] = [1, 2, 3]
    omd.json["k"] = [4, 5, 6]

    omd.pkl.clear()

    assert len(omd.pkl) == 0
    assert omd.json["k"] == [4, 5, 6]
    assert len(omd.json) == 1


def test_clear_one_of_many_formats_preserves_others(tmp_path):
    """Clearing one format among three leaves the other two intact."""
    omd = OverlappingMultiDict(
        dict_type=FileDirDict,
        shared_subdicts_params=dict(base_dir=str(tmp_path)),
        pkl={},
        json={},
        txt=dict(base_class_for_values=str),
    )

    omd.pkl["data"] = {"a": 1}
    omd.json["data"] = {"b": 2}
    omd.txt["data"] = "text_value"

    omd.json.clear()

    assert len(omd.json) == 0
    assert omd.pkl["data"] == {"a": 1}
    assert omd.txt["data"] == "text_value"


def test_clear_local_dict_is_format_scoped():
    """clear() on LocalDict-backed OverlappingMultiDict is format-scoped."""
    omd = OverlappingMultiDict(
        dict_type=LocalDict,
        shared_subdicts_params=dict(),
        pkl={},
        json={},
    )

    omd.pkl["k1"] = "pkl_val"
    omd.json["k1"] = "json_val"
    omd.json["k2"] = "json_only"

    omd.json.clear()

    assert len(omd.json) == 0
    assert omd.pkl["k1"] == "pkl_val"
    assert len(omd.pkl) == 1
