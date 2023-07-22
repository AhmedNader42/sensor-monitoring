import random
from collections import namedtuple
import matplotlib.pyplot as plt

acc_band = namedtuple("AccBand", "min max acc_low acc_high")

acc_bands = [acc_band(0, 40, 3, 10), acc_band(41, 90, 7, 12), acc_band(91, 120, 6, 8)]
print(acc_bands)


def generator():
    target = random.randint(0, 100)
    s = 0
    print("TARGET IS : " + str(target))

    simulated_run = []
    while s < target:
        band_start = 0
        band_end = 0
        acc_low = 0
        acc_high = 0
        acc = 0
        for each in acc_bands:
            band_start, band_end, acc_low, acc_high = each
            if s in range(band_start, band_end + 1):
                acc = random.randint(acc_low, acc_high)
        s += acc
        simulated_run.append(s)

        # if random.random() < 0.1:
        #     yield "!~@! Corrupted Record !~@!"
        # else:
        #     yield random.randint(68, 93) + random.random()

    yield simulated_run

    # for _ in range(0, 100):
    # print(next(generator()))


simulated = next(generator())
print(simulated)
plt.xticks(
    list(range(0, len(simulated) + 1)), [str(i) for i in range(0, len(simulated) + 1)]
)
plt.plot(simulated)
print(max(simulated))
plt.grid()
plt.show()
