import getopt, sys
from sumset import *

args = sys.argv[1:]
options = "hsS"
long_options = ["Help", "set"]

q = 1
try:
    arguments, values = getopt.getopt(args, options, long_options)
    for currentArg, currentVal in arguments:
        if currentArg in ("-h", "--Help"):
            print("python3 set_info.py -s 1 2 3 --> Sumset([1, 2, 3])")
        elif currentArg in ("-s", "-S"):
            s = list(sys.argv[2:])
            for i in range(0, len(s)):
                s[i] = int(s[i])
            q = 0
except getopt.error as err:
    print(str(err))

if q == 0 and isinstance(s, list):
    S = Sumset(s)
    print("S = " + str(S.set))
    print("Cardinality of S: " + str(len(S.set)))
    print("Doubling constant of S: " + str(S.doubling_constant))
