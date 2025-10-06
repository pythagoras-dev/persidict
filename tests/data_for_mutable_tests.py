from persidict import FileDirDict, BasicS3Dict, LocalDict, S3Dict_FileDirCached


mutable_tests = [

(FileDirDict, dict(serialization_format="pkl", digest_len=11))
,(FileDirDict, dict(serialization_format="json", digest_len=11))
,(S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="my_bucket", digest_len=11))
,(S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="her_bucket", digest_len=11))


,(FileDirDict, dict(serialization_format="pkl", digest_len=5))
,(FileDirDict, dict(serialization_format="json", digest_len=5))
,(S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="my_bucket", digest_len=5))
,(S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="his_bucket", digest_len=5))


,(FileDirDict, dict(serialization_format="pkl", digest_len=0))
,(FileDirDict, dict(serialization_format="json", digest_len=0))
,(S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="my_bucket", digest_len=0))
,(S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="her_bucket", digest_len=0))


,(FileDirDict, dict(serialization_format="pkl"))
,(FileDirDict, dict(serialization_format="json"))
,(S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="her_bucket"))
,(S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="their_bucket"))
,(BasicS3Dict, dict(serialization_format="pkl", bucket_name="super_bucket"))
,(BasicS3Dict, dict(serialization_format="json", bucket_name="mega_bucket"))
,(LocalDict, dict(serialization_format="json", bucket_name="first_bucket"))
,(LocalDict, dict(serialization_format="pkl", bucket_name="second_bucket"))


,(S3Dict_FileDirCached, dict(serialization_format="pkl", bucket_name="a_bucket", root_prefix ="_"))
,(S3Dict_FileDirCached, dict(serialization_format="json", bucket_name="the_bucket", root_prefix ="OYO"))
,(BasicS3Dict, dict(serialization_format="pkl", bucket_name="a_bucket", root_prefix = "_"))
,(BasicS3Dict, dict(serialization_format="json", bucket_name="the_bucket", root_prefix = "OYO"))

]