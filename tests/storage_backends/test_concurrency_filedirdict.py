import multiprocessing
import random
import time

import pytest

from persidict import FileDirDict
from persidict.jokers_and_status_flags import ETagIfExists, ETAG_IS_THE_SAME

# Protect multiprocessing code on Windows from infinite recursion
multiprocessing.freeze_support()

pytestmark = pytest.mark.slow

def many_operations(base_dir:str, process_n:int):
    d = FileDirDict(base_dir=base_dir)
    d["a"] = random.random()
    for i in range(50):
        try:
            time.sleep(random.random())
            for j in range(50):
                if random.random() < 0.5:
                    d["a"] = random.random()
                else:
                    _ = d["a"]
        except Exception as e:
            d[f"error_in_process_{i}_{e.__class__.__name__}"] = True

def _conditional_write_with_pause(
    base_dir: str,
    key: str,
    etag: ETagIfExists,
    ready_event: multiprocessing.Event,
    proceed_event: multiprocessing.Event,
    result_queue: multiprocessing.Queue,
):
    d = FileDirDict(base_dir=base_dir)
    original_save = d._save_to_file

    def slow_save(file_name, value, **kwargs):
        ready_event.set()
        proceed_event.wait(5)
        return original_save(file_name, value, **kwargs)

    d._save_to_file = slow_save
    result_queue.put(d.set_item_if(key, value="conditional", condition=ETAG_IS_THE_SAME, expected_etag=etag))

def _unconditional_write(
    base_dir: str,
    key: str,
    start_event: multiprocessing.Event,
    started_event: multiprocessing.Event,
):
    d = FileDirDict(base_dir=base_dir)
    if not start_event.wait(5):
        return
    started_event.set()
    d[key] = "unconditional"

def test_concurrency_5(tmpdir):
    base_dir = str(tmpdir)
    processes = []
    for i in range(5):
        p = multiprocessing.Process(target=many_operations, args=(base_dir,i,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    d = FileDirDict(base_dir=base_dir)
    assert len(d) == 1, f"Expected 1 item, found {len(d)} items: {list(d.keys())}"
    assert "a" in d
    assert isinstance(d["a"], float)



def test_concurrency_10(tmpdir):
    base_dir = str(tmpdir)
    processes = []
    for i in range(10):
        p = multiprocessing.Process(target=many_operations, args=(base_dir,i,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    d = FileDirDict(base_dir=base_dir)
    assert len(d) == 1, f"Expected 1 item, found {len(d)} items: {list(d.keys())}"
    assert "a" in d
    assert isinstance(d["a"], float)

def test_conditional_and_unconditional_write_can_race(tmpdir):
    base_dir = str(tmpdir)
    key = "mixed_key"
    d = FileDirDict(base_dir=base_dir)
    d[key] = "initial"
    etag = d.etag(key)

    ready_event = multiprocessing.Event()
    proceed_event = multiprocessing.Event()
    started_event = multiprocessing.Event()
    result_queue = multiprocessing.Queue()

    p_conditional = multiprocessing.Process(
        target=_conditional_write_with_pause,
        args=(base_dir, key, etag, ready_event, proceed_event, result_queue),
    )
    p_unconditional = multiprocessing.Process(
        target=_unconditional_write,
        args=(base_dir, key, ready_event, started_event),
    )

    p_conditional.start()
    try:
        assert ready_event.wait(5)
        p_unconditional.start()
        assert started_event.wait(5)
        time.sleep(0.1)
    finally:
        proceed_event.set()
        for proc in (p_unconditional, p_conditional):
            proc.join(5)
            if proc.is_alive():
                proc.terminate()
                proc.join()

    assert p_conditional.exitcode == 0
    assert p_unconditional.exitcode == 0
    assert result_queue.get(timeout=1) is not None
    assert FileDirDict(base_dir=base_dir)[key] in {"conditional", "unconditional"}
