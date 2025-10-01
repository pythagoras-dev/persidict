import time

from persidict import FileDirDict, S3Dict

def min_sleep(dct: FileDirDict | S3Dict) -> None:
    """ Sleep for a minimum time to ensure different timestamps. """
    if isinstance(dct,FileDirDict):
        time.sleep(0.17)
    elif isinstance(dct, S3Dict):
        time.sleep(1.1)
    else:
        raise ValueError(f"Unknown dict type: {type(dct)}")
