# persidict Tests: Map and Navigation

This file is the human index to the test suite. It is intentionally small and
feature-oriented so it is quick to scan.

## How to use this map
- Find the feature area, then open the listed test files.
- When adding or moving tests, update this file and `tests/ownership.yaml`.
- If a file covers multiple areas, list it in the primary area and add a short
  note in the test docstring.

## Feature map (current)

- Core dict API and behavior:
  - tests/core_behavior/test_basics.py
  - tests/core_behavior/test_advanced_dict_methods.py
  - tests/core_behavior/test_update_method.py
  - tests/core_behavior/test_copy_methods.py
  - tests/core_behavior/test_iterators.py
  - tests/core_behavior/test_setdefault.py
  - tests/core_behavior/test_delete_current.py
  - tests/core_behavior/test_delete_if_exists.py
  - tests/core_behavior/test_discard.py
  - tests/core_behavior/test_discard_empty_dict.py
  - tests/core_behavior/test_append_only_delete.py
  - tests/core_behavior/test_append_only_setitem.py
  - tests/core_behavior/test_empty_dict.py
  - tests/core_behavior/test_bool.py
  - tests/core_behavior/test_equality.py
  - tests/core_behavior/test_case_sensitivity.py
  - tests/core_behavior/test_random_key.py
  - tests/core_behavior/test_ior.py
  - tests/core_behavior/test_keep_current.py
  - tests/core_behavior/test_unpicklable.py
  - tests/core_behavior/test_typing.py
  - tests/core_behavior/test_jokers.py
  - tests/core_behavior/test_validate_returned_value.py
  - tests/core_behavior/test_pop_no_redundant_read.py
  - tests/core_behavior/test_popitem.py
  - tests/core_behavior/test_get_value_and_etag_base.py
  - tests/core_behavior/test_exception_contracts.py

- Key handling and safe tuples:
  - tests/key_handling/test_safe_chars.py
  - tests/key_handling/test_safe_str_tuple.py
  - tests/key_handling/test_complex_keys.py
  - tests/key_handling/test_path_traversal.py

- Subdicts:
  - tests/subdictionary_operations/test_subdicts.py
  - tests/subdictionary_operations/test_subdicts_method.py
  - tests/subdictionary_operations/test_get_subdict.py

- Storage (FileDirDict/LocalDict/UNC):
  - tests/storage_backends/test_digest_length_matrix.py
  - tests/storage_backends/test_filedirdict_setdefault.py
  - tests/storage_backends/test_filedirdict_etag_fallback.py
  - tests/storage_backends/test_filedirdict_etag_consistency.py
  - tests/storage_backends/test_concurrency_filedirdict.py
  - tests/storage_backends/test_filedirdict_retry.py
  - tests/storage_backends/test_local_dict.py
  - tests/storage_backends/test_local_dict_etag_shared_counter.py
  - tests/storage_backends/test_discard_local_dict.py
  - tests/storage_backends/test_serialization_format_validation.py
  - tests/storage_backends/test_utf8_encoding.py
  - tests/storage_backends/test_unc_support.py
  - tests/storage_backends/test_filedirdict_iteration_races.py
  - tests/storage_backends/test_filedirdict_ignores_junk_files.py

- Simple Storage Service (S3-backed dicts):
  - tests/simple_storage_service/test_basic_s3_setdefault.py
  - tests/simple_storage_service/test_basic_s3_setdefault_race.py
  - tests/simple_storage_service/test_basic_s3_iteration_and_metadata.py
  - tests/simple_storage_service/test_root_prefix_behavior.py
  - tests/simple_storage_service/test_s3_append_only_etag.py
  - tests/simple_storage_service/test_s3_append_only_setitem_atomic.py
  - tests/simple_storage_service/test_s3_validate_returned_value.py
  - tests/simple_storage_service/test_actual_s3.py
  - tests/simple_storage_service/test_basic_s3_conditional_mismatch_returns_result.py

- Entity tag and conditional operations:
  - tests/entity_tag_operations/test_etag.py
  - tests/entity_tag_operations/conditional_operations_contract/
  - tests/entity_tag_operations/conditional_operations_contract/test_get_with_etag.py
  - tests/entity_tag_operations/conditional_operations_contract/test_etag_is_the_same_across_classes.py
  - tests/entity_tag_operations/conditional_operations_contract/test_retrieve_value_default.py
  - tests/entity_tag_operations/conditional_operations_contract/test_value_was_mutated.py
  - tests/entity_tag_operations/conditional_operations_contract/test_never_retrieve_skips_deserialization.py
  - tests/entity_tag_operations/conditional_operations_contract/test_setdefault_if_rejects_jokers.py
  - tests/entity_tag_operations/conditional_operations_contract/test_retrieve_value_with_keep_current.py
  - tests/entity_tag_operations/conditional_operations_contract/test_keep_current_conditional.py
  - tests/entity_tag_operations/conditional_operations_contract/test_delete_current_conditional.py
  - tests/entity_tag_operations/conditional_operations_contract/test_transform_item_retry_exhaustion.py
  - tests/entity_tag_operations/conditional_operations_contract/test_etag_is_the_same_all_methods.py
  - tests/entity_tag_operations/conditional_operations_contract/test_any_etag_all_methods.py
  - tests/entity_tag_operations/conditional_operations_contract/test_etag_has_changed_all_methods.py
  - tests/entity_tag_operations/conditional_operations_contract/test_item_not_available_expected_etag.py
  - tests/entity_tag_operations/conditional_operations_mutable/

- Variants (cached/append-only/write-once/multi-dict):
  - tests/dictionary_variants/test_append_only_dict_cached.py
  - tests/dictionary_variants/test_write_once_dict.py
  - tests/dictionary_variants/test_overlapping_multi_dict.py
  - tests/dictionary_variants/test_etaggable_dict_cached.py
  - tests/dictionary_variants/test_mutable_cached_conflict_cache_safety.py
  - tests/dictionary_variants/test_overlapping_multi_dict_clear_is_format_scoped.py

- Timestamps:
  - tests/timestamp_behavior/test_timestamp.py
  - tests/timestamp_behavior/test_timestamp_functions.py
  - tests/timestamp_behavior/test_timestamp_comprehensive.py

- Compatibility and serialization:
  - tests/compatibility_serialization/test_work_with_basic_datatypes.py
  - tests/compatibility_serialization/test_work_with_pandas.py
  - tests/compatibility_serialization/test_work_with_python_src.py

- Atomic types and optional deps:
  - tests/atomic_type_support/

- Versioning:
  - tests/version_behavior/

- Live actions (opt-in):
  - tests/__live_actions__/

- Helpers:
  - tests/conftest.py
  - tests/data_for_mutable_tests.py
  - tests/atomic_test_config.py
  - tests/minimum_sleep.py
