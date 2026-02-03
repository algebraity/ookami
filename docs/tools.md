# OOKAMI v1.1.0

*Licensed under GPL 3.0*

An implementation of methods and properties applicable to a diffset of the integers ( \mathbb{Z} ), along with methods for constructing them. Suitable for research in additive and multiplicative combinatorics on subsets of ( \mathbb{Z} ).

---

## tools.py

### Dependencies

The `tools` module depends on the following standard-library modules:

- `os`
- `csv`
- `time`
- `multiprocessing`
- `dataclasses`
- `typing` (used for type annotations)
- `numpy`

It also depends on the CombSet class from the `ookami` package:

- `CombSet` from `ookami.combset`

---

### Constants

- `HEADER: list[str]`  
  Column headers used when exporting powerset information to CSV. The columns correspond to basic combinatorial invariants of each set.

---

### Public Methods

#### `compute_powerset_info`

```python
compute_powerset_info(
    n: int,
    out_dir: str,
    jobs: int,
    k: int,
    flush_every: int,
    min_computation: bool,
    mp_context: str = "fork"
) -> None
````

Compute and export combinatorial information for all non-empty subsets of
({1, 2, \dots, n}).

The computation is parallelized using Python’s multiprocessing framework and split across multiple output files.

**Parameters**

* `n`
  Size of the ambient set ({1,\dots,n}).
* `out_dir`
  Directory where CSV files will be written.
* `jobs`
  Number of worker processes.
* `k`
  Number of disjoint chunks into which the powerset is partitioned.
* `flush_every`
  Number of rows buffered before writing to disk.
* `mp_context`
  Multiprocessing start method (e.g. `"fork"`, `"spawn"`).
* `min_computation`
  Whether or nor to perform a minimal vs full computation; True only writes `list(S._set)`, `(S.ads).cardinality`, and `(S.mds).cardinality`.

**Output**

* Writes up to `k` CSV files per invocation, each containing information about a disjoint subset of the powerset.
* Prints progress information to standard output.

**Exported Data**
Each row corresponds to a non-empty subset (A \subseteq {1,\dots,n}) and includes:

* The set (A)
* Cardinalities of (A+A), (A-A), and (A\cdot A)
* (|A|), diameter, and density
* Doubling constant
* Arithmetic and geometric progression indicators
* Additive and multiplicative energies

---

#### `rand_sets`

```python
rand_sets(
    num_sets: int,
    length: int,
    min_val: int,
    max_val: int
) -> list[CombSet]
```

Generate random subsets of the integers.

**Parameters**

* `num_sets`
  Number of random sets to generate.
* `length`
  Number of elements in each set.
* `min_val`
  Minimum possible element.
* `max_val`
  Maximum possible element.

**Returns**

* A list of `CombSet` objects.

**Example**

```python
from ookami import tools

sets = tools.rand_sets(10, 10, 1, 100)
```

---

#### `rand_sums`

```python
rand_sums(
    num_sums: int,
    length1: int,
    length2: int,
    min1: int,
    min2: int,
    max1: int,
    max2: int
) -> list[tuple[CombSet, CombSet, CombSet]]
```

Generate random sumsets of pairs of random sets.

**Parameters**

* `num_sums`
  Number of random sums to generate.
* `length1`, `length2`
  Cardinalities of the two summand sets.
* `min1`, `max1`
  Range for elements of the first set.
* `min2`, `max2`
  Range for elements of the second set.

**Returns**

* A list of tuples `(A, B, A+B)` where `A` and `B` are random `CombSet` objects.

**Example**

```python
from ookami import tools

sums = tools.rand_sums(10, 10, 10, 1, 1, 100, 100)
```

---

### Internal Helpers

The following functions and classes are internal and not part of the public API:

* `_mask_to_subset(mask: int, n: int) -> tuple[int, ...]`
* `_compute_row(subset: tuple[int, ...]) -> list`
* `_worker(task: WorkerTask) -> str`
* `_export_powerset_info(...)`
* `WorkerTask` (dataclass encapsulating worker parameters)

These are implementation details used to support parallel powerset enumeration and CSV export.

---

### Intended Use

The `tools` module is designed for:

* Large-scale combinatorial data generation
* Empirical exploration of sum-product phenomena
* Benchmarking additive and multiplicative invariants
* Producing datasets for downstream statistical or symbolic analysis

It is not required for basic use of OOKAMI’s core algebraic functionality.

