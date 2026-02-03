from __future__ import annotations

import random as rand
from fractions import Fraction
import numpy as np
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

class CombSet():
    def __init__(self, base_set: Optional[Union[Sequence[int], np.ndarray]] = None) -> None:
        if base_set is not None:
            if isinstance(base_set, list):
                if base_set == []:
                    raise ValueError("base_set cannot be empty!")
            if isinstance(base_set, np.ndarray):
                if base_set.size == 0:
                    raise ValueError("base_set cannot be empty!")

            self._set = base_set
            self._normalize()
        
        self.add_cache = {}
        self.diff_cache = {}
        self.mult_cache = {}
        self.rep_add_cache = {}
        self.rep_diff_cache = {}
        self.rep_mult_cache = {}
        self.energies = {}

        if base_set is None:
            self.construct()

    def add(self, x: int) -> None:
        self._set = np.append(self._set, np.int64(x))
        self._normalize()
        self._clear_cache()

    def remove(self, x: int) -> None:
        x = np.int64(x)
        if self.cardinality > 0:
            self._set = self._set[self._set != x]
        if self._set.size == 0:
            raise ValueError("self._set cannot be empty!")
        self._normalize()
        self._clear_cache()

    def construct(self, nums: Optional[Union[Sequence[int], np.ndarray]] = None) -> None:
        if nums == []:
            raise ValueError("nums cannot be empty.")

        if nums is not None:
            self._set = nums
            self._normalize()
            self._clear_cache()
        else:
            self._set = []
            choice = int(input("Please enter a number to select one of the following options: \n 0: First n elements of kN (enter n, k later) \n 1: Custom set (enter each element later)\nYour choice: "))
            print("\n")
            if choice == 0:
                k = int(input("Enter k as an integer: "))
                n = int(input("Enter n as an integer: "))
                self._set = [k * i for i in range(1, n+1)]
            elif choice == 1:
                current_input = ""
                while True:
                    current_input = input("\nEnter the next element of the set (or \"stop\"): ")
                    if current_input == "stop":
                        break
                    else:
                        i = int(current_input)
                        self._set.append(i)
            else:
                raise ValueError("Invalid input. Selected option must be 0 or 1.")

            print("Set created.")

            if self._set == []:
                raise ValueError("self._set cannot be empty!")
            self._normalize()
            self._clear_cache()

    def translate(self, n: int) -> CombSet:
        n = np.int64(n)
        return CombSet(self._set + n)

    def rep_add(self, x: int, k: int = 2) -> int:
        if (k, int(x)) in self.rep_add_cache:
            return self.rep_add_cache[(k, int(x))]

        x = np.int64(x)
        vals = np.add.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.add.outer(vals, self._set).reshape(-1, self._set.size)
        rep = int(np.count_nonzero(vals == x))
        self.rep_add_cache[(k, int(x))] = rep
        return self.rep_add_cache[(k, int(x))]

    def rep_diff(self, x: int, k: int = 2) -> int:
        if (k, int(x)) in self.rep_diff_cache:
            return self.rep_diff_cache[(k, int(x))]

        x = np.int64(x)
        vals = np.subtract.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.subtract.outer(vals, self._set).reshape(-1, self._set.size)
        rep = int(np.count_nonzero(vals == x))
        self.rep_diff_cache[(k, int(x))] = rep
        return self.rep_diff_cache[(k, int(x))]

    def rep_mult(self, x: int, k: int = 2) -> int:
        if (k, int(x)) in self.rep_mult_cache:
            return self.rep_mult_cache[(k, int(x))]

        x = np.int64(x)
        vals = np.multiply.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.multiply.outer(vals, self._set).reshape(-1, self._set.size)
        rep = int(np.count_nonzero(vals == x))
        self.rep_mult_cache[(k, int(x))] = rep
        return self.rep_mult_cache[(k, int(x))]

    def rand_set(self, length: int = 0, min_element: int = 0, max_element: int = 0) -> None:
        if length == 0:
            raise ValueError("length must be greater than 0.")
        gen_set = []
        if max_element - min_element + 1 < length:
            raise(ValueError("Length higher than range of possible values."))
        while len(gen_set) < length:
            r = rand.randint(min_element, max_element)
            if not r in gen_set:
                gen_set.append(r)

        self._set = gen_set
        self._normalize()
        self._clear_cache()        

    def info(self, n: int = -1) -> Dict[str, Any]:
        self_sum = self.ads
        self_diff = self.dds
        self_prod = self.mds
        card = self.cardinality
        diam = self.diameter
        densty = self.density
        dc = self.doubling_constant
        is_ap = self.is_arithmetic_progression
        is_gp = self.is_geometric_progression
        add_energy = self.energy_add
        mult_energy = self.energy_mult
        if n > 1:
            sum_list = []
            for i in range(2, n+1):
                sum_list.append(i*self)
            return {"add_ds": self_sum, "diff_ds": self_diff, "mult_ds": self_prod, "cardinality": card, "diameter": diam, "density": densty, "dc": dc, "is_ap": is_ap, "is_gp": is_gp, "add_energy": add_energy, "mult_energy": mult_energy, "i*A_list": sum_list}

        return {"add_ds": self_sum, "diff_ds": self_diff, "mult_ds": self_prod, "cardinality": card, "diameter": diam, "density": densty, "dc": dc, "is_ap": is_ap, "is_gp": is_gp, "add_energy": add_energy, "mult_energy": mult_energy}

    @property
    def cardinality(self):
        return int(self._set.size)
        
    @property
    def diameter(self):
        return int(self._set[-1] - self._set[0])

    @property
    def density(self):
        return Fraction(self.cardinality, int(self._set[-1] - self._set[0] + 1))

    @property
    def ads(self):
        if not 2 in self.add_cache:
            self.add_cache[2] = 2*self
        return self.add_cache[2]
    
    @property
    def dds(self):
        if not 2 in self.diff_cache:
            self.diff_cache[2] = self - self
        return self.diff_cache[2]

    @property
    def mds(self):
        if not 2 in self.mult_cache:
            self.mult_cache[2] = self**2
        return self.mult_cache[2]

    @property
    def ads_cardinality(self):
        return int((self.ads)._set.size)

    @property
    def dds_cardinality(self):
        return int((self.dds)._set.size)

    @property
    def mds_cardinality(self):
        return int((self.mds)._set.size)

    @property
    def doubling_constant(self):
        num = int((self.ads)._set.size)
        denom = int(self._set.size)
        return Fraction(num, denom)

    @property
    def is_arithmetic_progression(self):
        if self._set.size == 1:
            return True
        d = self._set[1] - self._set[0]
        return bool(np.all(np.diff(self._set) == d))

    @property
    def is_geometric_progression(self):
        if self._set.size == 1:
            return True
        if np.any(self._set == 0):
            return False
        a0 = int(self._set[0])
        a1 = int(self._set[1])
        r = Fraction(a1, a0)
        for i in range(2, int(self._set.size)):
            if Fraction(int(self._set[i]), int(self._set[i-1])) != r:
                return False
        return True

    @property
    def energy_add(self):
        return self.k_energy_add(2)

    def k_energy_add(self, k: int) -> int:
        if ("add", int(k)) in self.energies:
            return self.energies[("add", int(k))]

        vals = np.add.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.add.outer(vals, self._set).reshape(-1, self._set.size)
        _, counts = np.unique(vals.ravel(), return_counts=True)
        energy = int(np.sum(counts.astype(np.int64) ** 2))
        self.energies[("add", int(k))] = energy
        return self.energies[("add", int(k))]

    @property
    def energy_diff(self):
        return self.k_energy_diff(2)

    def k_energy_diff(self, k: int) -> int:
        if ("diff", int(k)) in self.energies:
            return self.energies[("diff", int(k))]

        vals = np.subtract.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.subtract.outer(vals, self._set).reshape(-1, self._set.size)
        _, counts = np.unique(vals.ravel(), return_counts=True)
        energy = int(np.sum(counts.astype(np.int64) ** 2))
        self.energies[("diff", int(k))] = energy
        return self.energies[("diff", int(k))]

    @property
    def energy_mult(self):
        return self.k_energy_mult(2)

    def k_energy_mult(self, k: int) -> int:
        if ("mult", int(k)) in self.energies:
            return self.energies[("mult", int(k))]

        vals = np.multiply.outer(self._set, self._set)
        for _ in range(int(k) - 2):
            vals = np.multiply.outer(vals, self._set).reshape(-1, self._set.size)
        _, counts = np.unique(vals.ravel(), return_counts=True)
        energy = int(np.sum(counts.astype(np.int64) ** 2))
        self.energies[("mult", int(k))] = energy
        return self.energies[("mult", int(k))]

    def _clear_cache(self) -> None:
        self.add_cache = {}
        self.diff_cache = {}
        self.mult_cache = {}
        self.rep_add_cache = {}
        self.rep_diff_cache = {}
        self.rep_mult_cache = {}
        self.energies = {}

    def _normalize(self) -> None:
        if isinstance(self._set, np.ndarray):
            a = self._set
        else:
            a = np.asarray(self._set, dtype=np.int64)
        if a.size == 0:
            raise ValueError("self._set cannot be empty!")
        if a.dtype != np.int64:
            a = a.astype(np.int64, copy=False)
        a = np.unique(a)
        self._set = a

    def __add__(self, other: Union[CombSet, int]) -> CombSet:
        if not isinstance(other, CombSet):
            if isinstance(other, int):
                return self.translate(other)
            raise(TypeError)
        if self is other:
            if 2 in self.add_cache:
                return self.add_cache[2]
        new_set = np.add.outer(self._set, other._set).ravel()
        if self is other:
            self.add_cache[2] = CombSet(new_set)
            return self.add_cache[2]
        return CombSet(new_set)

    def __sub__(self, other: Union[CombSet, int]) -> CombSet:
        if not isinstance(other, CombSet):
            if isinstance(other, int):
                return self.translate(-other)
            raise(TypeError)
        if self is other:
            if 2 in self.diff_cache:
                return self.diff_cache[2]
        new_set = np.subtract.outer(self._set, other._set).ravel()
        if self is other:
            self.diff_cache[2] = CombSet(new_set)
            return self.diff_cache[2]
        return CombSet(new_set)

    def __rmul__(self, other: int) -> CombSet:
        if isinstance(other, int):
            if other == 0:
                return CombSet([0])
            if other in self.add_cache:
                return self.add_cache[other]
            if other < 0:
                result = -(abs(other) * self)
                self.add_cache[other] = result
                return result
            result = CombSet(self._set.copy())
            times = other-1
            while times > 0:
                result += self
                times -= 1
            self.add_cache[other] = result
            return result
        raise TypeError("Multiplication is only supported for CombSet * CombSet, int * CombSet, and CombSet * int.")

    def __mul__(self, other: Union[int, CombSet]) -> CombSet:
        if isinstance(other, int):
            if other == 0:
                return CombSet([0])
            new_set = self._set * np.int64(other)
            return CombSet(new_set)
        if isinstance(other, CombSet):
            if self is other:
                if 2 in self.mult_cache:
                    return self.mult_cache[2]
            prod_set = np.multiply.outer(self._set, other._set).ravel()
            if self is other:
                self.mult_cache[2] = CombSet(prod_set)
                return self.mult_cache[2]
            return CombSet(prod_set)
        raise TypeError("Multiplication is only supported for CombSet * CombSet, int * CombSet, and CombSet * int.")

    def __pow__(self, other: int) -> CombSet:
        if isinstance(other, int):
            if other == 0:
                return CombSet([1])
            if other < 0:
                raise TypeError("Negative exponentiation is not supported.")
            if other in self.mult_cache:
                return self.mult_cache[other]
            current = CombSet(self._set.copy())
            n = other
            while n > 1:
                current *= self
                n -= 1
            self.mult_cache[other] = current
            return self.mult_cache[other]
        raise TypeError("Exponentiation is only supported for CombSet ** int.")


    def __str__(self) -> str:
        return "CombSet(" + str(self._set.tolist()) + ")"

    def __iter__(self) -> Iterator[int]:
        return (int(x) for x in self._set)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CombSet):
            return bool(np.array_equal(self._set, other._set))
        else:
            return False
        
    def __neg__(self) -> CombSet:
        return CombSet((-self._set))

    __repr__ = __str__
