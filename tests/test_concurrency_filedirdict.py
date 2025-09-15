import random, time, multiprocessing

from persidict import FileDirDict

# Protect multiprocessing code on Windows from infinite recursion
multiprocessing.freeze_support()

def many_operations(base_dir:str, process_n:int):
    d = FileDirDict(base_dir)
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


