import random as rand
from fractions import Fraction

############################################################################################################################
#                                                                                                                          #
# self.set (list): The mathematical set represented by the Sumset object.                                                  #
#                                                                                                                          #
# self.construct(nums=None): Constructs a set, either by taking a list as input to the method, or by taking user input.    #
# self.rand_set(length=0, min_element=0, max_element=0): Generates a random Sumset with the paramaters given.              #
# self.doubling_constant: A property giving |A + A|/|A| as a Fraction object.                                              #
#                                                                                                                          #
# self.__add__(): Add two Sumset objects as sumset. A + B = {a + b : a in A, b in B}.                                      #
# self.__rmul__(): 3 * A = A + A + A                                                                                       #
# self.__mul__(): A * 3 = {3a : a in A}                                                                                    #
#                                                                                                                          #
############################################################################################################################
class Sumset():
    def __init__(self, base_set=None):
        self.set = base_set
        
        if base_set is None:
            self.construct()

    def construct(self, nums=None):
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
                while current_input != -1:
                    current_input = int(input("\nEnter the next element of the set (or \"-1\"): "))
                    if isinstance(current_input, int) and current_input != -1:
                        self.set.append(current_input)

            print("Set created.")

        self.set = list(set(self.set))
        self.set = sorted(self.set)

    def rand_set(self, length=0, min_element=0, max_element=0):
        gen_set = []
        if max_element - min_element + 1 < length:
            raise(ValueError("Length higher than range of possible values."))
        while len(gen_set) < length:
            r = rand.randint(min_element, max_element)
            if not r in gen_set:
                gen_set.append(r)

        gen_set = sorted(gen_set)
        self.set = gen_set

    @property
    def doubling_constant(self):
        ApA = self + self
        num = len(ApA.set)
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

    def __add__(self, other):
        if not isinstance(other, Sumset):
            raise(TypeError)

        new_set = []
        for a in self.set:
            for b in other.set:
                c = a+b
                if c not in new_set:
                    new_set.append(c)

        new_set = list(set(new_set))
        new_set = sorted(new_set)
        return Sumset(new_set)

    def __rmul__(self, other):
        if isinstance(other, int):
            result = Sumset(self.set)
            times = other-1
            while times > 0:
                result += self
                times -= 1
            return result

    def __mul__(self, other):
        if isinstance(other, int):
            new_set = self.set
            for i in range(0, len(new_set)):
                new_set[i] *= other
            return Sumset(new_set)

    def __str__(self):
        self.set = list(set(self.set))
        self.set = sorted(self.set)
        return "Sumset(" + str(self.set) + ")"

    __repr__ = __str__
