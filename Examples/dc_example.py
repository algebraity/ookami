from sumset import *

N = 100

doubling_constants = []

for n in range(2, N):
    S = Sumset([i for i in range(1, n)])
    S2 = Sumset([n**2 + i for i in range(1, n)])

    set_to_study = S + S2
    doubling_constants.append(set_to_study.doubling_constant)

print(doubling_constants)
