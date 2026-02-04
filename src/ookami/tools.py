import os
import csv
import time
import random as rand
import multiprocessing as mp
from dataclasses import dataclass
from ookami import CombSet
from typing import Any, List, Tuple, Union

HEADER = [
    "set", "add_ds_card", "diff_ds_card", "mult_ds_card",
    "set_cardinality", "diameter", "density", "dc",
    "is_ap", "is_gp", "add_energy", "mult_energy"
]

MIN_HEADER = [
    "set", "add_ds_card", "mult_ds_card"
]


def _mask_to_subset(mask: int, n: int) -> tuple[int, ...]:
    return tuple(i + 1 for i in range(n) if (mask >> i) & 1)


def _compute_row(subset: tuple[int, ...], mask: int) -> List[Any]:
    S = CombSet(subset)
    info = S.info()
    return [
        mask,
        info["add_ds"].cardinality,
        info["diff_ds"].cardinality,
        info["mult_ds"].cardinality,
        info["cardinality"],
        info["diameter"],
        info["density"],
        str(info["dc"]),
        info["is_ap"],
        info["is_gp"],
        info["add_energy"],
        info["mult_energy"],
    ]

def _compute_row_min(subset: tuple[int, ...], mask: int) -> List[Any]:
    S = CombSet(subset)
    return [
        mask,
        (S.ads).cardinality,
        (S.mds).cardinality
    ]

@dataclass(frozen=True)
class WorkerTask:
    chunk_id: int
    n: int
    k: int
    flush_every: int
    out_dir: str
    minimal: bool


def _worker(task: WorkerTask) -> str:
    chunk_id, n, k, flush_every, out_dir = (
        task.chunk_id, task.n, task.k, task.flush_every, task.out_dir
    )

    total = 1 << n
    file_id = chunk_id+1
    path = os.path.join(out_dir, f"set_info_{n}_{file_id:04d}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if task.minimal:
            w.writerow(MIN_HEADER)
            to_call = _compute_row_min
        else:
            w.writerow(HEADER)
            to_call = _compute_row

        buf: list[list] = []
        for mask in range(chunk_id, total, k):
            if mask == 0:
                continue
            subset = _mask_to_subset(mask, n)
            buf.append(to_call(subset, mask))

            if len(buf) >= flush_every:
                w.writerows(buf)
                buf.clear()

        if buf:
            w.writerows(buf)
            buf.clear()

    return path


def _export_powerset_info(n: int, out_dir: str, jobs: int, k: int, flush_every: int, min_computation: bool = False, mp_context: str = "fork") -> None:
    if n < 1:
        raise ValueError("n must be >= 1")
    if jobs < 1:
        raise ValueError("jobs must be >= 1")
    if k < 1:
        raise ValueError("k must be >= 1")
    if flush_every < 1:
        raise ValueError("flush_every must be >= 1")

    os.makedirs(out_dir, exist_ok=True)

    t0 = time.time()

    try:
        ctx = mp.get_context(mp_context)
    except ValueError:
        ctx = mp.get_context()

    tasks = [WorkerTask(i, n, k*jobs, flush_every, out_dir, min_computation) for i in range(k*jobs)]

    with ctx.Pool(processes=jobs) as pool:
        done = 0
        for path in pool.imap_unordered(_worker, tasks, chunksize=1):
            done += 1
            print(f"{(100*done)//(k*jobs)}% done, wrote {path}, {time.time()-t0:.1f}s since start")

compute_powerset_info = _export_powerset_info

def rand_sums(num_sums: int, length1: int, length2: int, min1: int, min2: int, max1: int, max2: int) -> List[Tuple[CombSet, CombSet, CombSet]]:
    results = []
    for _ in range(0, num_sums):
        S1 = CombSet([0])
        S2 = CombSet([0])
        S1.rand_set(length=length1, min_element=min1, max_element=max1)
        S2.rand_set(length=length2, min_element=min2, max_element=max2)

        results.append((S1, S2, S1 + S2))

    return(results)

def rand_sets(num_sets: int, length: int, min_val: int, max_val: int) -> List[CombSet]:
    sets: List[CombSet] = []
    for i in range(0, num_sets):
        S = CombSet([0])
        S.rand_set(length, min_val, max_val)
        sets.append(S)

    return sets

def rand_ap(starts: Union[int, Tuple[int, int], List[int]], d: Union[int, Tuple[int, int], List[int]], length: Union[int, Tuple[int, int], List[int]], num_aps: int = 1) -> List[CombSet]:
    aps: List[CombSet] = []

    if num_aps < 1:
        raise ValueError("Number of APs must be at least 1")

    if isinstance(starts, int):
        pass
    elif isinstance(starts, (tuple, list)) and len(starts) == 2 and starts[0] <= starts[1]:
        pass
    else:
        raise ValueError("starts must be an int or a (lo, hi) pair")

    if isinstance(d, int):
        if d < 1:
            raise ValueError("Common difference must be at least 1")
    elif isinstance(d, (tuple, list)) and len(d) == 2 and d[0] <= d[1] and d[0] >= 1:
        pass
    else:
        raise ValueError("d must be an int >= 1 or a (lo, hi) pair with lo >= 1")

    if isinstance(length, int):
        if length < 1:
            raise ValueError("length must be at least 1")
    elif isinstance(length, (tuple, list)) and len(length) == 2 and 1 <= length[0] <= length[1]:
        pass
    else:
        raise ValueError("length must be an int >= 1 or a (lo, hi) pair with lo >= 1")

    starts_is_int = isinstance(starts, int)
    d_is_int = isinstance(d, int)
    length_is_int = isinstance(length, int)

    for _ in range(num_aps):
        ap_start = starts if starts_is_int else rand.randint(starts[0], starts[1])
        ap_d = d if d_is_int else rand.randint(d[0], d[1])
        ap_len = length if length_is_int else rand.randint(length[0], length[1])

        aps.append(CombSet([ap_start + ap_d * i for i in range(ap_len)]))

    if len(aps) == 1:
        return aps[0]
    return aps

def rand_gp(starts: Union[int, Tuple[int, int], List[int]], r: Union[int, Tuple[int, int], List[int]], length: Union[int, Tuple[int, int], List[int]], num_gps: int = 1) -> List[CombSet]:
    gps: List[CombSet] = []

    if num_gps < 1:
        raise ValueError("Number of GPS must be at least 1")
    if isinstance(starts, int):
        pass
    elif isinstance(starts, (tuple, list)) and len(starts) == 2 and starts[0] <= starts[1]:
        pass
    else:
        raise ValueError("starts must be an int or a (lo, hi) pair")

    if isinstance(r, int):
        if r < 1:
            raise ValueError("Common ratio must be at least 1")
    elif isinstance(r, (tuple, list)) and len(r) == 2 and isinstance(r[0], int) and isinstance(r[1], int) and r[0] <= r[1] and r[0] >= 1:
        pass
    else:
        raise ValueError("r must be an int >= 1 or a (lo, hi) pair with lo >= 1")

    if isinstance(length, int):
        if length < 1:
            raise ValueError("length must be at least 1")
    elif isinstance(length, (tuple, list)) and len(length) == 2 and 1 <= length[0] <= length[1]:
        pass
    else:
        raise ValueError("length must be an int >= 1 or a (lo, hi) pair with lo >= 1")

    starts_is_int = isinstance(starts, int)
    r_is_int = isinstance(r, int)
    length_is_int = isinstance(length, int)

    for _ in range(num_gps):
        gp_start = starts if starts_is_int else rand.randint(starts[0], starts[1])
        gp_r = r if r_is_int else rand.randint(r[0], r[1])
        gp_len = length if length_is_int else rand.randint(length[0], length[1])

        gps.append(CombSet([gp_start * gp_r ** i for i in range(gp_len)]))
    
    if len(gps) == 1:
        return gps[0]
    return gps
