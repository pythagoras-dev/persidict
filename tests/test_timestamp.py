import time

from moto import mock_aws
from persidict import FileDirDict, S3Dict


@mock_aws
def test_timestamp(tmpdir):
    """test timestamp methods."""
    for d in [
        FileDirDict(base_dir= tmpdir.mkdir("LOCAL"))
        ,S3Dict(base_dir = tmpdir.mkdir("AWS"), bucket_name ="mybucket")
        ]:
        for v in "abcdefg":
            d[v] = 5*v
            #wait one second
            time.sleep(1)

        assert d["a"] == "aaaaa"
        del d["b"]
        del d["f"]
        tmstmp = d.timestamp("a")
        for i in range(5):
            assert d.timestamp("a") == tmstmp
        oldest = d.oldest_keys(3)
        assert oldest == ['a', 'c', 'd']
        assert [d[i] for i in oldest] == ['aaaaa', 'ccccc', 'ddddd']

        newest = d.newest_keys(3)
        assert newest == ['g', 'e', 'd']
        assert [d[i] for i in newest] == ['ggggg', 'eeeee', 'ddddd']

        assert d.newest_keys(100) == list(reversed(d.oldest_keys(100)))