
import json

from mixinforge import access_jsparams, dumpjs, loadjs, update_jsparams
from persidict.jokers_and_status_flags import (
    DELETE_CURRENT,
    DeleteCurrentFlag,
    KEEP_CURRENT,
    KeepCurrentFlag,
    _DeleteCurrent,
    _KeepCurrent,
)

def test_Jokers():
    """Test if KeepCurrentFlag is a singleton.
    """
    assert _KeepCurrent is KeepCurrentFlag()
    assert KEEP_CURRENT is KeepCurrentFlag()
    KEEP_CURRENT_1 = KeepCurrentFlag()
    assert KEEP_CURRENT is KEEP_CURRENT_1

    assert _DeleteCurrent is DeleteCurrentFlag()
    assert DELETE_CURRENT is DeleteCurrentFlag()
    DELETE_CURRENT_1 = DeleteCurrentFlag()
    assert DELETE_CURRENT is DELETE_CURRENT_1

    assert KEEP_CURRENT is not DELETE_CURRENT


def test_jokers_dumpjs_loadjs_roundtrip_singletons():
    for flag_cls, instances in (
        (KeepCurrentFlag, (_KeepCurrent, KEEP_CURRENT)),
        (DeleteCurrentFlag, (_DeleteCurrent, DELETE_CURRENT)),
    ):
        for instance in instances:
            restored = loadjs(dumpjs(instance))
            for expected in instances:
                assert restored is expected
            assert restored is flag_cls()


def test_jokers_dumpjs_loadjs_in_collections():
    payload = {
        "keep": KEEP_CURRENT,
        "delete": DELETE_CURRENT,
        "list": [KEEP_CURRENT, DELETE_CURRENT],
    }

    restored = loadjs(dumpjs(payload))

    assert restored["keep"] is KEEP_CURRENT
    assert restored["delete"] is DELETE_CURRENT
    assert restored["list"][0] is KEEP_CURRENT
    assert restored["list"][1] is DELETE_CURRENT


def test_jokers_access_jsparams_handles_jokers():
    payload = {
        "keep": KEEP_CURRENT,
        "delete": DELETE_CURRENT,
        "nested": {"keep": KEEP_CURRENT},
    }

    jsparams = dumpjs(payload)
    accessed = access_jsparams(jsparams, "keep", "delete", "nested")

    assert loadjs(dumpjs(accessed["keep"])) is KEEP_CURRENT
    assert loadjs(dumpjs(accessed["delete"])) is DELETE_CURRENT

    nested = loadjs(dumpjs(accessed["nested"]))
    assert nested["keep"] is KEEP_CURRENT


def test_jokers_update_jsparams_preserves_jokers():
    payload = {
        "keep": KEEP_CURRENT,
        "delete": DELETE_CURRENT,
        "note": "old",
    }

    jsparams = dumpjs(payload)

    updated_jsparams = update_jsparams(jsparams, note="new")
    restored = loadjs(updated_jsparams)

    assert restored["keep"] is KEEP_CURRENT
    assert restored["delete"] is DELETE_CURRENT
    assert restored["note"] == "new"