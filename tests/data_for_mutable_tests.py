from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached


# Minimal matrix for broad contract coverage across backends.
mutable_tests = [
    (FileDirDict, dict(serialization_format="pkl")),
    (FileDirDict, dict(serialization_format="json")),
    (LocalDict, dict(serialization_format="pkl", bucket_name="local_bucket")),
    (LocalDict, dict(serialization_format="json", bucket_name="local_bucket")),
    (S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="cache_bucket")),
    (S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="cache_bucket")),
    (BasicS3Dict, dict(serialization_format="pkl", bucket_name="basic_bucket")),
    (BasicS3Dict, dict(serialization_format="json", bucket_name="basic_bucket")),
]

# Targeted matrices for configuration edge coverage.
mutable_tests_digest_len = [
    (FileDirDict, dict(serialization_format="pkl", digest_len=0)),
    (FileDirDict, dict(serialization_format="json", digest_len=5)),
    (FileDirDict, dict(serialization_format="pkl", digest_len=11)),
    (S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="digest_bucket", digest_len=5)),
]

mutable_tests_root_prefix = [
    (S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="prefix_bucket", root_prefix="_")),
    (BasicS3Dict, dict(serialization_format="json", bucket_name="prefix_bucket", root_prefix="OYO")),
]

mutable_tests_extended = mutable_tests_digest_len + mutable_tests_root_prefix
