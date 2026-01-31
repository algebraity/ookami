import os
import csv
import json
import time
import multiprocessing as mp
from dataclasses import dataclass
from ookami import CombSet


HEADER = [
    "set", "add_ds_card", "diff_ds_card", "mult_ds_card",
    "set_cardinality", "diameter", "density", "dc",
    "is_ap", "is_gp", "add_energy", "mult_energy"
]


def mask_to_subset(mask: int, n: int) -> tuple[int, ...]:
    return tuple(i + 1 for i in range(n) if (mask >> i) & 1)


def compute_row(subset: tuple[int, ...]) -> list:
    S = CombSet(subset)
    info = S.info()
    return [
        json.dumps(S._set),
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


@dataclass(frozen=True)
class WorkerTask:
    chunk_id: int
    n: int
    k: int
    flush_every: int
    out_dir: str


def _worker(task: WorkerTask) -> str:
    chunk_id, n, k, flush_every, out_dir = (
        task.chunk_id, task.n, task.k, task.flush_every, task.out_dir
    )

    total = 1 << n
    path = os.path.join(out_dir, f"set_info_{n}_{chunk_id:04d}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)

        buf: list[list] = []
        for mask in range(chunk_id, total, k):
            if mask == 0:
                continue
            subset = mask_to_subset(mask, n)
            buf.append(compute_row(subset))

            if len(buf) >= flush_every:
                w.writerows(buf)
                buf.clear()

        if buf:
            w.writerows(buf)
            buf.clear()

    return path


def export_powerset_info(n, out_dir, jobs, k, flush_every, mp_context="fork"):

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

    tasks = [WorkerTask(i, n, k, flush_every, out_dir) for i in range(k)]

    with ctx.Pool(processes=jobs) as pool:
        done = 0
        for path in pool.imap_unordered(_worker, tasks, chunksize=1):
            done += 1
            print(f"{(100*done)//k}% done, wrote {path}, {time.time()-t0:.1f}s since start")

compute_powerset_info = export_powerset_info

def rand_sums(num_sums, length1, length2, min1, min2, max1, max2):
    results = []
    for _ in range(0, num_sums):
        S1 = CombSet([])
        S2 = CombSet([])
        S1.rand_set(length=length1, min_element=min1, max_element=max1)
        S2.rand_set(length=length2, min_element=min2, max_element=max2)

        results.append((S1, S2, S1 + S2))

    return(results)

def rand_sets(num_sets, length, min_val, max_val):
    sets = []
    for i in range(0, num_sets):
        sets.append(CombSet([0]).rand_set(length, min_val, max_val))

    return sets

