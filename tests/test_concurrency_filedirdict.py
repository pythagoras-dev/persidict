import random, time, multiprocessing

from persidict import FileDirDict

def many_operations(dir_name:str, process_n:int):
    d = FileDirDict(dir_name)
    d["a"] = random.random()
    for i in range(50):
        try:
            time.sleep(random.random())
            for j in range(50):
                if random.random() < 0.5:
                    d["a"] = random.random()
                else:
                    _ = d["a"]
        except:
            d[f"error_in_process_{i}"] = True

def test_concurrency(tmpdir):
    dir_name = str(tmpdir)
    processes = []
    for i in range(25):
        p = multiprocessing.Process(target=many_operations, args=(dir_name,i,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    d = FileDirDict(dir_name)
    assert len(d) == 1
    assert "a" in d
    assert isinstance(d["a"], float)


