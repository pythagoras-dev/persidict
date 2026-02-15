"""Tests that a failed conditional write on MutableDictCached does not poison the cache.

When set_item_if fails because the ETag condition is not satisfied, the cache
must never contain the proposed (rejected) value. Instead, the cache should
reflect the actual current state of the main store.
"""

import pytest

from persidict.cached_mutable_dict import MutableDictCached
from persidict.local_dict import LocalDict
from persidict.jokers_and_status_flags import (
    ETAG_IS_THE_SAME,
    ALWAYS_RETRIEVE,
)


@pytest.fixture()
def cached_env():
    main = LocalDict(serialization_format="json")
    data_cache = LocalDict(serialization_format="pkl")
    etag_cache = LocalDict(serialization_format="json")
    wrapper = MutableDictCached(main_dict=main, data_cache=data_cache, etag_cache=etag_cache)
    return main, data_cache, etag_cache, wrapper


def test_failed_write_does_not_cache_proposed_value(cached_env):
    """Cache must not contain the proposed value after a failed conditional write."""
    main, data_cache, etag_cache, wrapper = cached_env

    wrapper["k"] = "v1"
    old_etag = wrapper.etag("k")

    # Externally mutate the main store, creating an ETag mismatch
    main["k"] = "v2"
    current_etag = main.etag("k")
    assert current_etag != old_etag

    result = wrapper.set_item_if(
        "k",
        value="PROPOSED_SHOULD_NOT_APPEAR",
        condition=ETAG_IS_THE_SAME,
        expected_etag=old_etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert not result.condition_was_satisfied
    assert result.new_value == "v2"
    assert result.actual_etag == current_etag

    # The cache must reflect the actual value, not the proposed one
    assert data_cache["k"] == "v2"
    assert wrapper["k"] == "v2"
    assert data_cache["k"] != "PROPOSED_SHOULD_NOT_APPEAR"


def test_failed_write_updates_etag_cache_to_actual(cached_env):
    """After a failed conditional write, the ETag cache reflects the real ETag."""
    main, data_cache, etag_cache, wrapper = cached_env

    wrapper["k"] = "original"
    stale_etag = wrapper.etag("k")

    main["k"] = "externally_updated"
    real_etag = main.etag("k")

    result = wrapper.set_item_if(
        "k",
        value="rejected",
        condition=ETAG_IS_THE_SAME,
        expected_etag=stale_etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert not result.condition_was_satisfied
    assert etag_cache["k"] == real_etag


def test_successful_write_updates_cache_correctly(cached_env):
    """Successful conditional write stores the new value in cache."""
    main, data_cache, etag_cache, wrapper = cached_env

    wrapper["k"] = "v1"
    etag = wrapper.etag("k")

    result = wrapper.set_item_if(
        "k",
        value="v2",
        condition=ETAG_IS_THE_SAME,
        expected_etag=etag,
        retrieve_value=ALWAYS_RETRIEVE,
    )

    assert result.condition_was_satisfied
    assert data_cache["k"] == "v2"
    assert wrapper["k"] == "v2"
