import random as rand
from fractions import Fraction

#############################################################################################################################
#                                                                                                                           #
# SUMSET CLASS -- v0.1.1 (Alpha)                                                                                            #
#                                                                                                                           #
# An implementation of methods and properties applicable to a subset of the integers Z, along with methods of               #
# constructing them. Suitable for research in additive and multiplicative combinatorics on subsets of Z.                    #
#                                                                                                                           #
# self.set (list): the mathematical set represented by the Sumset object                                                    #
# self.add_cache (dict): a cache storing computed values of i*A                                                             #
# self.sub_cache (dict): a cache storing computed values of (-i)A                                                           #
# self.mult_cache (dict): a cache storing computed values of A**i                                                           #
#                                                                                                                           #
# self.construct(nums=None): constructs a set, either by taking a list as input to the method, or by taking user input.     #
# self.translate(n): translates the set A by n, returning A + {n} = {a + n : a in A}                                        #
# self.rand_set(length=0, min_element=0, max_element=0): generates a random Sumset with the paramaters given.               #
# self.info(n): returns a dictionary containing all computable information about the set available in the Sumset class,     #
#               including the list [2*A, 3*A, ..., n*A]                                                                     #
#                                                                                                                           #
# self.cardinality: property representing |A|                                                                               #
# self.diameter: property giving the diameter of A                                                                          #
# self.density: property giving |A|/(maxA - minA + 1)                                                                       #
# self.ads: property returning A+A, caching it if not yet computed and reading from self.add_cache otherwise                #
# self.sds: property returning A-A, caching it if not yet computed and reading from self.sub_cahce otherwise                #
# self.mds: property returning A*A, caching it if not yet computed and reading from self.mult_cache otherwise               #
# self.ads_cardinality: property giving |A+A|                                                                               #
# self.mds_cardinality: property giving |A*A|                                                                               #
# self.sds_cardinality: property giving |A-A|                                                                               $
# self.doubling_constant: property giving |A + A|/|A| as a Fraction object.                                                 #
# self.is_arithmetic_progression: property returning True if the Sumset object is an arithmetic progression, False o/w.     #
# self.is_geometric_progression: property returning True if the Sumset object is a geometric progression, False o/w.        #
# self.additive_energy: property returning the additive energy E(A) of a Sumset object                                      #
# self.multiplicative_energy: property returning the multiplicative energy of a Sumset object                               #
#                                                                                                                           #
# self._clear_cache(): clears self.add_cache and self.mult_cache                                                            #
# self._normalize(): sets self.set = sorted(set(self.set))                                                                  #
#                                                                                                                           #
# self.__add__(): add two Sumset objects as sumset. A + B = {a + b : a in A, b in B}                                        #
# self.__rmul__(): 3 * A = A + A + A                                                                                        #
# self.__mul__(): A * 3 = {3a : a in A}                                                                                     #
#                 A * B = {a * b : a in A, b in B}                                                                          #
# self.__pow__(): A ** n = A * A * ... * A (negative powers unsupported)                                                    #
# self.__eq__(): A == B if and only if A.set == B.set                                                                       #
# self.__neq__(): -A = {-a : a in A}                                                                                        #
#                                                                                                                           #
#############################################################################################################################
class Sumset():
    def __init__(self, base_set=None):
        if base_set is not None:
            if base_set == []:
                raise ValueError("base_set cannot be empty!")
            self.set = base_set
            self._normalize()
        self.add_cache = {}
        self.sub_cache = {}
        self.mult_cache = {}
        
        if base_set is None:
            self.construct()

    def construct(self, nums=None):
        if nums == []:
            raise ValueError("nums cannot be empty.")        
        
        if nums is not None:
            self.set = nums
        else:
            self.set = []
            choice = int(input("Please enter a number to select one of the following options: \n 0: First n elements of kN (enter n, k later) \n 1: Custom set (enter each element later)\nYour choice: "))
            print("\n")
            if choice == 0:
                k = int(input("Enter k as an integer: "))
                n = int(input("Enter n as an integer: "))
                self.set = [k * i for i in range(1, n+1)]
            elif choice == 1:
                current_input = ""
                while True:
                    current_input = input("\nEnter the next element of the set (or \"stop\"): ")
                    if current_input == "stop":
                        break
                    else:
                        i = int(current_input)
                        self.set.append(i)
                if self.set == []:
                    raise ValueError("self.set cannot be empty!")

            print("Set created.")

        self._clear_cache()
        self._normalize()

    def translate(self, n):
        translated_set = []
        for i in range(0, len(self.set)):
            translated_set.append(self.set[i] + n)

        return Sumset(translated_set)

    def rep_add(k=2):
        pass

    def rep_mult(k=2):
        pass

    def rand_set(self, length=0, min_element=0, max_element=0):
        if length == 0:
            raise ValueError("length must be greater than 0.")
        gen_set = []
        if max_element - min_element + 1 < length:
            raise(ValueError("Length higher than range of possible values."))
        while len(gen_set) < length:
            r = rand.randint(min_element, max_element)
            if not r in gen_set:
                gen_set.append(r)

        self.set = gen_set
        self._normalize()
        self._clear_cache()        

    def info(self, n=-1):
        self_sum = self.ads
        self_diff = self.sds
        self_prod = self.mds
        card = self.cardinality
        diam = self.diameter
        densty = self.density
        dc = self.doubling_constant
        is_ap = self.is_arithmetic_progression
        is_gp = self.is_geometric_progression
        ae = self.additive_energy
        me = self.multiplicative_energy
        if n > 1:
            sum_list = []
            for i in range(2, n+1):
                sum_list.append(i*self)
            return {"add_ds": self_sum, "sub_ds": self_diff, "mult_ds": self_prod, "cardinality": card, "diameter": diam, "density": densty, "dc": dc, "is_ap": is_ap, "is_gp": is_gp, "additive_energy": ae, "mult_energy": me, "i*A_list": sum_list}

        return {"additive_doubling_set" : self_sum, "sub_ds": self_diff, "mult_doubling_set": self_prod, "cardinality": card, "diameter": diam, "density": densty, "dc": dc, "is_ap": is_ap, "is_gp": is_gp, "additive_energy": ae, "mult_energy": me}

    @property
    def cardinality(self):
        return len(self.set)
        
    @property
    def diameter(self):
        return (self.set[-1] - self.set[0])

    @property
    def density(self):
        return self.cardinality / (self.set[-1] - self.set[0] + 1)

    @property
    def ads(self):
        if not 2 in self.add_cache:
            self.add_cache[2] = 2*self
        return self.add_cache[2]
    
    @property
    def sds(self):
        if not 2 in self.sub_cache:
            self.sub_cache[2] = self - self
        return self.sub_cache[2]

    @property
    def mds(self):
        if not 2 in self.mult_cache:
            self.mult_cache[2] = self**2
        return self.mult_cache[2]

    @property
    def ads_cardinality(self):
        return len((self.ads).set)

    @property
    def sds_cardinality(self):
        return len((self.sds).set)

    @property
    def mds_cardinality(self):
        return len((self.mds).set)

    @property
    def doubling_constant(self):
        num = len((self.ads).set)
        denom = len(self.set)
        return Fraction(num, denom)

    @property
    def is_arithmetic_progression(self):
        if len(self.set) == 1:
            return True
        else:
            d = self.set[1] - self.set[0]
            for i in range(2, len(self.set)):
                if self.set[i] - self.set[i-1] != d:
                    return False
            return True

    @property
    def is_geometric_progression(self):
        if len(self.set) == 1:
            return True
        elif 0 in self.set:
            return False
        else:
            r = self.set[1] / self.set[0]
            for i in range(2, len(self.set)):
                if self.set[i] / self.set[i-1] != r:
                    return False
            return True

    @property
    def additive_energy(self):
        r_vals = {}
        for a in self:
            for b in self:
                if a + b in r_vals.keys():
                    r_vals[a + b] += 1
                else:
                    r_vals[a + b] = 1
        energy = 0
        for r in r_vals:
            energy += r_vals[r]**2

        return energy

    @property
    def multiplicative_energy(self):
        r_vals = {}
        for a in self:
            for b in self:
                if a*b in r_vals.keys():
                    r_vals[a*b] += 1
                else:
                    r_vals[a*b] = 1
        energy = 0
        for r in r_vals:
            energy += r_vals[r]**2

        return energy

    def _clear_cache(self):
        self.add_cache = {}
        self.sub_cache = {}
        self.mult_cache = {}

    def _normalize(self):
        self.set = sorted(set(self.set))

    def __add__(self, other):
        if not isinstance(other, Sumset):
            if isinstance(other, int):
                return self.translate(other)
            raise(TypeError)
        if other == self:
            if 2 in self.add_cache:
                return self.add_cache[2]
        new_set = []
        for a in self.set:
            for b in other.set:
                c = a+b
                if c not in new_set:
                    new_set.append(c)

        return Sumset(new_set)

    def __sub__(self, other):
        if not isinstance(other, Sumset):
            if isinstance(other, int):
                return self.translate(-other)
            raise(TypeError)
        if other == self:
            if 2 in self.sub_cache:
                return self.sub_cache[2]
        new_set = []
        for a in self.set:
            for b in other.set:
                c = a-b
                if c not in new_set:
                    new_set.append(c)

        return Sumset(new_set)

    def __rmul__(self, other):
        if isinstance(other, int):
            if other == 0:
                return Sumset([0])
            elif other in self.add_cache:
                return self.add_cache[other]
            elif other in self.sub_cache:
                return self.sub_cache[other]
            result = Sumset(self.set)
            times = abs(other) - 1
            while times > 0:
                if other > 0:
                    result += self
                elif other < 0:
                    result -= self
                times -= 1
            if other > 0:
                self.add_cache[other] = result
            elif other < 0:
                self.sub_cache[other] = result
            return result
        if isinstance(other, Sumset):
            prod_set = []
            for i in self:
                for j in other:
                    if not i*j in prod_set:
                        prod_set.append(i*j)

            prod_set = sorted(set(prod_set))
            return Sumset(prod_set)

    def __mul__(self, other):
        if isinstance(other, int):
            if other == 0:
                return Sumset([0])
            new_set = (self.set).copy()
            for i in range(0, len(new_set)):
                new_set[i] *= other
            return Sumset(new_set)
        if isinstance(other, Sumset):
            prod_set = []
            for i in self:
                for j in other:
                    if not i*j in prod_set:
                        prod_set.append(i*j)

            return Sumset(prod_set)

    def __pow__(self, other):
        if isinstance(other, int):
            if other == 0:
                return Sumset([1])
            if other < 0:
                raise ValueError("Negative exponentiation is not supported.")
            if other in self.mult_cache:
                return self.mult_cache[other]
            current = Sumset(self.set)
            n = other
            while n > 1:
                current *= self
                n -= 1

            self.mult_cache[other] = current
            return self.mult_cache[other]

    def __str__(self):
        return "Sumset(" + str(self.set) + ")"

    def __iter__(self):
        return iter(self.set)

    def __eq__(self, other):
        if isinstance(other, Sumset):
            return self.set == other.set
        else:
            return False
        
    def __neg__(self):
        return Sumset([-i for i in self.set])

    __repr__ = __str__