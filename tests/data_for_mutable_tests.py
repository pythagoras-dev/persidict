from src.persidict import FileDirDict, S3Dict

mutable_tests = [

(FileDirDict, dict(file_type="pkl", digest_len=11))
,(FileDirDict, dict(file_type="json", digest_len=11))
,(S3Dict, dict(file_type="pkl", bucket_name="my_bucket", digest_len=11))
,(S3Dict, dict(file_type="json", bucket_name="her_bucket", digest_len=11))

,(FileDirDict, dict(file_type="pkl", digest_len=5))
,(FileDirDict, dict(file_type="json", digest_len=5))
,(S3Dict, dict(file_type="pkl", bucket_name="my_bucket", digest_len=5))
,(S3Dict, dict(file_type="json", bucket_name="his_bucket", digest_len=5))

,(FileDirDict, dict(file_type="pkl", digest_len=0))
,(FileDirDict, dict(file_type="json", digest_len=0))
,(S3Dict, dict(file_type="pkl", bucket_name="my_bucket", digest_len=0))
,(S3Dict, dict(file_type="json", bucket_name="her_bucket", digest_len=0))

,(FileDirDict, dict(file_type="pkl"))
,(FileDirDict, dict(file_type="json"))
,(S3Dict, dict(file_type="pkl", bucket_name="her_bucket"))
,(S3Dict, dict(file_type="json", bucket_name="their_bucket"))

,(S3Dict, dict(file_type="pkl", bucket_name="a_bucket", root_prefix = "_"))
,(S3Dict, dict(file_type="json", bucket_name="the_bucket", root_prefix = "OYO"))

]
