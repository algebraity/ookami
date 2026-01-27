import getopt, sys
from sumset import *

args = sys.argv[1:]
options = "sSnN"
long_options = ["set", "num"]

num = 1
try:
    if args[0] == "-n" or args[0] == "-N":
        num = int(args[1])
        if len(args) > 2:
            if args[2] == "-s":
                string = args[3].split()
                s = [int(i) for i in string]
        else:
            print("Need to specify a set!")
    elif args[0] == "-s" or args[0] == "-S":
        string = args[1].split()
        s = [int(i) for i in string]
        if len(args) > 2:
            num = int(args[3])
except getopt.error as err:
    print(str(err))

if isinstance(s, list):
    S = Sumset(s)
    computed = []
    if num > 1:
        for i in range(2, num+1):
            computed.append(i*S)
            
    print("S = " + str(S.set))
    print("Cardinality of S: " + str(len(S.set)))
    print("Doubling constant of S: " + str(S.doubling_constant))
    print("Is arithmetic progression: " + str(S.is_arithmetic_progression))
    print("Additive energy: " + str(S.additive_energy))
    if num > 1:
        print("iS for 2 <= i <= " + str(num) + ": ")
        for i in range(0, len(computed)):
            print("  " + str(i+2) + "*S = " + str(computed[i]))
