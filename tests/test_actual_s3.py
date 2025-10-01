# from moto import mock_aws
#
# from persidict import PersiDict, S3Dict_Legacy
#
#
# def test_actual_s3():
#
#     s3dict = S3Dict_Legacy(bucket_name='pythagorassamos') #, region="us-west-2")
#     s3dict.clear()
#     assert len(s3dict) == 0
#
#     N = 20
#
#     for i in range(N):
#         s3dict[f'a_{i}'] = 100_000_000*i
#
#     assert len(s3dict) == N
#     s3dict.clear()
#     assert len(s3dict) == 0