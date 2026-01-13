import random as rand

class Sumset():
    def __init__(self, base_set=None):
        self.set = base_set
        
        if base_set == None:
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
            elif choice == -1:
                current_input = ""
                while current_input != -1:
                    current_input = int(input("\nEnter the next element of the set (or \"-1\"): "))
                    if isinstance(current_input, int):
                        self.set.append(current_input)

            print("Set created.")

        self.set = list(set(self.set))
        self.set = sorted(self.set)

    def rand_set(self, length=0, min_element=0, max_element=0):
        gen_set = []
        if max_element - min_element + 1 < length:
            length = max_element - min_element + 1
        while len(gen_set) < length:
            r = rand.randint(min_element, max_element)
            if not r in gen_set:
                gen_set.append(r)

        self.set = gen_set

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

    def __str__(self):
        return "Sumset(" + str(self.set) + ")"

    __repr__ = __str__
