![OOKAMI logo](https://media.discordapp.net/attachments/1416510086024663080/1466854379360354418/ookami.png?ex=697e4218&is=697cf098&hm=f4986fb7364763ffec24bfc38389323c113e767c8f365e0e1a15bdd86e8cb430&=&format=webp&quality=lossless&width=1316&height=482)

# OOKAMI - v1.1.0

The purpose of this project is to provide a set of tools that can be used for computations with
subsets of the integers in additive and multiplicative combinatorics. It is designed with my own research goals
in mind, and thus it may not meet the needs of other projects exactly, but it is well-documented so that 
others may use it for their own work. The project is entirely Python-based; see [Dependencies](https://github.com/algebraity/ookami?tab=readme-ov-file#dependencies) for a list of all dependencies.

OOKAMI is under active development. While its core functionality is stable, the API may evolve, and users are recommended to consult the source before using it for production work. While this 
README file provides basic descriptions and demonstrations of OOKAMI's features, full documentation is available within the `docs` directory, which is included in every release.

For questions about licensing, see [License and attribution](https://github.com/algebraity/ookami#license-and-attribution).

## Dependencies

The `ookami.combset` module requires only the `random` and `fractions` packages by default, while `ookami.tools` requires the `os`, `csv`, `json`, `time`, `multiprocessing`, and `dataclasses` packages. These packages are all a part of the Python standard library, so having a recent version of Python3 installed should be enough to run OOKAMI.

## Installation

OOKAMI is a Python package which can be installed with `pip`. To install the current stable release of OOKAMI, make sure you have `pip`
installed on your system. Download the latest release from the GitHub Releases page, extract it, and run:
```bash
cd ookami-<version>
pip install .
```
After this process, OOKAMI can be used in a Python shell or any Python program simply by importing the
`ookami` package.

For developers interested in the latest version of OOKAMI, run:
```bash
git clone https://github.com/algebraity/ookami.git
cd ookami
pip install -e .
```
This is not recommended for most users.

For documentation on what OOKAMI includes and how to use it, read the markdown files in the `docs` directory.

## Features
* Represent a finite set of integers with a `CombSet` object
* Manipulate the underlying set `CombSet._set` through operations `CombSet.add(x)` and `CombSet.remove(x)` (direct manipulation of CombSet._set is not supported)
* Form sumsets: `A + B = {a + b; a in A, b in B}`
* Translation by a constant: `A.translate(x) = {a + x : a in A}`
* Repeated addition with self: `n*A = A + A + ... + A`
* Scalar dilation: `A*n = {n*a : a in A}`
* Compute additive, difference, and multiplicative representation functions
* Compute invariants of a set
  * Cardinality: `CombSet.cardinality`
  * Diameter: `CombSet.diameter`
  * Density: `CombSet.density`
  * A+A: `A.ads`
  * A-A: `A.dds`
  * A*A: `A.mds`
  * |A+A|: `A.ads_cardinality`
  * |A-A|: `A.dds_cardinality`
  * |A*A|: `A.mds_cardinality`
  * Doubling constant: `CombSet.doubling_constant`
  * Is AP (True/False): `CombSet.is_arithmetic_progression`
  * Is GP (True/False): `CombSet.is_geometric_progression`
  * Ordered additive energy: `CombSet.energy_add`
  * Multiplicative energy: `CombSet.energy_mult`
* Return invariants as a dictionary with `CombSet.info(n)`
* Results of operations with a set and itself are cached for future use
* Computational tools including computing the properties of power sets, generating random sets, and generating random sums are available through the `tools` module
  
## Usage examples

Usage of `CombSet` class
```python
from ookami import CombSet

A = CombSet([1, 2, 3])
2*A                                # CombSet([2, 3, 4, 5, 6])
A*2                                # CombSet([2, 4, 6])

B = CombSet([1, 5])
A + B                              # CombSet([2, 3, 4, 6, 7, 8])

A.doubling_constant                # Fraction(5, 3)
A.is_arithmetic_progression        # True
A.is_geometric_progression         # False
A.energy_add                  # 19

A.info()                           # {'add_ds': CombSet([2, 3, 4, 5, 6]), 'diff_ds': CombSet([-2, -1, 0, 1, 2]), 'mult_ds': CombSet([1, 2, 3, 4, 6, 9]), 'cardinality': 3, 'diameter': 2, 'density': Fraction(1, 1), 'dc': Fraction(5, 3), 'is_ap': True, 'is_gp': False, 'add_energy': 19, 'mult_energy': 15}
A.info(3)                          # {'add_ds': CombSet([2, 3, 4, 5, 6]), 'diff_ds': CombSet([-2, -1, 0, 1, 2]), 'mult_ds': CombSet([1, 2, 3, 4, 6, 9]), 'cardinality': 3, 'diameter': 2, 'density': Fraction(1, 1), 'dc': Fraction(5, 3), 'is_ap': True, 'is_gp': False, 'add_energy': 19, 'mult_energy': 15, 'i*A_list': [CombSet([2, 3, 4, 5, 6]), CombSet([3, 4, 5, 6, 7, 8, 9])]}
```
Example usage of random set generation using the `ookami.tools` module
```python
from ookami import tools

# input: (num_sums, length, min_val, max_val)
tools.random_sets(10, 10, 1, 100)
# Generates 10 random sets of length 10, containing integers from 1 to 100 and
# returns them as a list

# input: (num_sums, length1, length2, min1, min2, max1, max2)
tools.random_sums(10, 10, 10, 1, 1, 100, 100)           
# Generates 10 random sums, each of two sets of length length1 and length2
# respectively, with min and max elements min1, max1 and min2, max2 respectively
```
Example use of `ookami.tools.compute_powerset_info`
```bash
[algebraity@T460 ~]$ python3 -i
Python 3.14.2 (main, Jan  2 2026, 14:27:39) [GCC 15.2.1 20251112] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from ookami import *
>>> compute_powerset_info(15, "data", 4, 5, 4000)   # input: (n, out_dir, jobs, k, buffer_size)
20% done, wrote data/set_info_15_0003.csv, 1.0s since start
40% done, wrote data/set_info_15_0002.csv, 1.0s since start
60% done, wrote data/set_info_15_0001.csv, 1.0s since start
80% done, wrote data/set_info_15_0004.csv, 1.0s since start
100% done, wrote data/set_info_15_0005.csv, 1.5s since start
>>> 
[algebraity@T460 ~]$ head data/set_info_15_0001.csv  -n 6
set,add_ds_card,diff_ds_card,mult_ds_card,set_cardinality,diameter,density,dc,is_ap,is_gp,add_energy,mult_energy
"[1, 3]",3,3,3,2,2,2/3,3/2,True,True,6,6
"[2, 4]",3,3,3,2,2,2/3,3/2,True,True,6,6
"[1, 2, 3, 4]",7,7,9,4,3,1,7/4,True,False,44,32
"[3, 5]",3,3,3,2,2,2/3,3/2,True,True,6,6
"[1, 4, 5]",6,7,6,3,4,3/5,2,False,False,15,15
# output: specific information about each (non-empty) subset of [15], split into up to k*jobs files
```

Example of `set_information.py` script
```bash
[algebraity@T460 scripts]$ python3 -i display_set_info.py -s "1 2 3" -n 5
S = [1, 2, 3]
Cardinality of S: 3
Diameter of S: 2
Density of S: 1.0
Doubling constant of S: 5/3
Is arithmetic progression: True
Is geometric progression: False
Additive energy: 19
Multiplicative energy: 15
iS for 2 <= i <= 5: 
  2*S = CombSet([2, 3, 4, 5, 6])
  3*S = CombSet([3, 4, 5, 6, 7, 8, 9])
  4*S = CombSet([4, 5, 6, 7, 8, 9, 10, 11, 12])
  5*S = CombSet([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
```

## License and attribution

The contents of this repository and the corresponding GitHub Releases page are licensed under the GNU General Public License v3.0 (GPL-3.0).

