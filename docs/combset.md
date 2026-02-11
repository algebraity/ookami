# OOKAMI v1.1.0

*Licensed under GPL 3.0*

An implementation of methods and properties applicable to a diffset of the integers ( \mathbb{Z} ), along with methods for constructing them. Suitable for research in additive and multiplicative combinatorics on subsets of ( \mathbb{Z} ).

---

## combset.py

### Dependencies

The 'combset' module depends on the following standard-library modules:

- 'random'
- 'fractions'
- 'typing' (used for type annotations)
- 'numpy'

---

### Attributes

* `self._set: np.ndarray`
  The mathematical set represented by the `CombSet` object.

* `self.add_cache: dict[int, "CombSet"]`
  Cache storing computed values of ( iA ).

* `self.diff_cache: dict[int, "CombSet"]`
  Cache storing computed values of ( A - A - \dots ).

* `self.mult_cache: dict[int, "CombSet"]`
  Cache storing computed values of ( A^i ).

* `self.rep_add_cache: dict[tuple[int,int], int]`
  Cache storing computed values of the ordered k-fold representation function r_{kA}(x); keys are `(k, x)`.

* `self.rep_diff_cache: dict[tuple[int,int], int]`
  Cache storing computed values of the ordered k-fold difference representation function; keys are `(k, x)`.

* `self.rep_mult_cache: dict[tuple[int,int], int]`
  Cache storing computed values of the ordered k-fold multiplicative representation function; keys are `(k, x)`.

* `self.energies: dict[str, int]`
  Stores additive and multiplicative energies once computed.

* `k_energy_add(self, k: int) -> int`  
  Compute the ordered k-fold additive energy E_k(A) = sum_x r_{kA}(x)^2 where r_{kA}(x) counts ordered representations of x as a sum of k elements of A. Results are cached in `self.energies` under the key `("add", k)`.

* `k_energy_diff(self, k: int) -> int`  
  Compute the ordered k-fold difference energy (analogous to additive energy but using repeated differences). Cached under `("diff", k)`.

* `k_energy_mult(self, k: int) -> int`  
  Compute the ordered k-fold multiplicative energy E_k^\times(A) = sum_x r_{A^{(k)}}(x)^2 where r counts ordered k-fold products. Cached under `("mult", k)`.

---

### Methods

* `add(self, x: int) -> None`
  Append an integer `x` to `self._set` safely.

* `remove(self, x: int) -> None`
  Remove an integer `x` from `self._set` safely.

* `construct(self, nums: list[int] | None = None) -> None`
  Construct a set, either from a provided list or via user input.

* `translate(self, n: int) -> "CombSet"`
  Translate the set ( A ) by `n`, returning
  ( A + {n} = {a + n : a \in A} ).

* `rep_add(self, x: int, k: int = 2) -> int`
  Return the ordered k-fold representation function r_{kA}(x) counting ordered representations of `x` as a sum of `k` elements of `A`. The argument order is `(x, k)` and `k` defaults to `2`.

* `rep_diff(self, x: int, k: int = 2) -> int`
  Return the ordered k-fold representation function counting ordered representations of `x` as an alternating difference of `k` elements (generalizing A-A). The argument order is `(x, k)` and `k` defaults to `2`.

* `rep_mult(self, x: int, k: int = 2) -> int`
  Return the ordered k-fold multiplicative representation function counting ordered representations of `x` as a product of `k` elements of `A`. The argument order is `(x, k)` and `k` defaults to `2`.

* `ruzsa_distance(self, other: "CombSet") -> float`
  The (additive) Ruzsa distance between two sets A and B defined by

  $$d(A,B)=\log\frac{|A-B|}{\sqrt{|A||B|}}$$

  - **Parameters:** `other` — a `CombSet` representing B.
  - **Returns:** `float` — the value of the Ruzsa distance. Returns `inf` when the denominator is zero (this is just to catch bugs, as the empty set is not supported).
  - **Notes:** Uses the implemented difference set `A - B` (i.e. `self - other`) and the cardinalities |A|, |B|. Matches `CombSet.ruzsa_distance` implementation.
  - **Example:** `A.ruzsa_distance(B)` computes `np.log((A - B).cardinality / (A.cardinality*B.cardinality)**0.5)`.

* `ruzsa_distance_positive(self, other: "CombSet") -> float`
  A closely related positive Ruzsa-type distance using the sumset instead of the difference set:

  $$d^+(A,B)=\log\frac{|A+B|}{\sqrt{|A||B|}}$$

  - **Parameters:** `other` — a `CombSet` representing B.
  - **Returns:** `float` — the value of the positive Ruzsa distance. Returns `inf` when the denominator is zero (this is just to catch bugs, as the empty set is not supported).
  - **Notes:** Uses the sumset `A + B` (i.e. `self + other`). Matches `CombSet.ruzsa_distance_positive` implementation.
  - **Example:** `A.ruzsa_distance_positive(B)` computes `np.log((A + B).cardinality / (A.cardinality*B.cardinality)**0.5)`.

* `rand_set(self, length: int = 0, min_element: int = 0, max_element: int = 0) -> "CombSet"`
  Generate a random `CombSet` with the given parameters.

* `info(self, n: int) -> dict[str, object]`
  Return a dictionary containing all computable information about the set available in the `CombSet` class, including the list
  ([2A, 3A, \dots, nA]).

---

### Properties

* `cardinality: int`
  The cardinality ( |A| ).

* `diameter: int`
  The diameter of ( A ).

* `density: float`
  The density ( |A| / (\max A - \min A + 1) ).

* `ads: "CombSet"`
  The sumset ( A + A ); cached after first computation.

* `dds: "CombSet"`
  The difference set ( A - A ); cached after first computation.

* `mds: "CombSet"`
  The product set ( A \cdot A ); cached after first computation.

* `ads_cardinality: int`
  The cardinality ( |A + A| ).

* `dds_cardinality: int`
  The cardinality ( |A - A| ).

* `mds_cardinality: int`
  The cardinality ( |A \cdot A| ).

* `doubling_constant: Fraction`
  The doubling constant ( |A + A| / |A| ).

* `is_arithmetic_progression: bool`
  `True` if the set is an arithmetic progression, `False` otherwise.

* `is_geometric_progression: bool`
  `True` if the set is a geometric progression, `False` otherwise.

* `energy_add: int`
  The additive energy ( E(A) ).

* `energy_mult: int`
  The multiplicative energy of the set.

---

### Internal Methods

* `_clear_cache(self) -> None`
  Clear `self.add_cache` and `self.mult_cache`.

* `_normalize(self) -> None`
  Normalize the set by assigning
  `self._set = np.asarray(self._set, dtype=object)`
  if not already an array, and then
  `self._set = np.unique(self._set)`

---

### Magic Methods

* `__add__(self, other: "CombSet") -> "CombSet"`
  Add two `CombSet` objects:
  ( A + B = {a + b : a \in A, b \in B} ).

* `__rmul__(self, n: int) -> "CombSet"`
  Scalar addition:
  ( n \cdot A = A + A + \dots + A ).

* `__mul__(self, other: int | "CombSet") -> "CombSet"`

  * ( A \cdot n = {na : a \in A} )
  * ( A \cdot B = {ab : a \in A, b \in B} )

* `__pow__(self, n: int) -> "CombSet"`
  Repeated product:
  ( A^n = A \cdot A \cdot \dots \cdot A )
  (negative powers unsupported).

* `__eq__(self, other: object) -> bool`
  Equality holds if and only if `self._set == other._set`.

* `__neg__(self) -> "CombSet"`
  Negation:
  ( -A = {-a : a \in A} ).

