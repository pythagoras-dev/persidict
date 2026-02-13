import inspect

from persidict import BasicS3Dict, FileDirDict, LocalDict, S3Dict_FileDirCached


def make_test_dict(dict_class, tmp_path=None, **kwargs):
    """Create a dict instance, filtering kwargs to only those accepted.

    Inspects the constructor of ``dict_class`` and passes only the keyword
    arguments it declares.  ``base_dir`` is injected automatically for
    classes whose constructor accepts it (e.g. FileDirDict,
    S3Dict_FileDirCached) when ``tmp_path`` is provided.
    """
    sig = inspect.signature(dict_class.__init__)
    accepted = set(sig.parameters.keys()) - {"self"}
    filtered = {k: v for k, v in kwargs.items() if k in accepted}
    if "base_dir" in accepted and tmp_path is not None:
        filtered.setdefault("base_dir", str(tmp_path))
    return dict_class(**filtered)


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
    (FileDirDict, dict(serialization_format="json", digest_len=4)),
    (FileDirDict, dict(serialization_format="json", digest_len=5)),
    (FileDirDict, dict(serialization_format="pkl", digest_len=11)),
    (S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="digest_bucket", digest_len=5)),
]

mutable_tests_root_prefix = [
    (S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="prefix_bucket", root_prefix="_")),
    (BasicS3Dict, dict(serialization_format="json", bucket_name="prefix_bucket", root_prefix="OYO")),
]

mutable_tests_extended = mutable_tests_digest_len + mutable_tests_root_prefix
