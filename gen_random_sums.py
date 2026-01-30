from .combset import CombSet

def gen_random_sums(num_sums=0, length1=0, length2=0, min1=0, min2=0, max1=0, max2=0):
    results = []
    for _ in range(0, num_sums):
        S1 = CombSet([])
        S2 = CombSet([])
        S1.rand_set(length=length1, min_element=min1, max_element=max1)
        S2.rand_set(length=length2, min_element=min2, max_element=max2)

        results.append((S1, S2, S1 + S2))

    print("\nResulting sets in the form (S1, S2, S1 + S2):")
    for result in results:
        print(result)
