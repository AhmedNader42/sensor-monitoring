import random
import numpy as np
import matplotlib.pyplot as plt
from collections import namedtuple
import math

acc_band = namedtuple("AccBand", "min max acc_low acc_high")

acc_bands = [
    acc_band(0, 40, 3, 10),
    acc_band(41, 90, 7, 12),
    acc_band(91, 200, 6, 8),
]
print(acc_bands)


def generator(current_speed) -> np.array:
    change_range = 40
    target: int = random.randint(
        max(current_speed - change_range, 0), min(current_speed + change_range, 180)
    )
    print("TARGET IS : " + str(target))
    speed = current_speed
    simulated_run = np.array([])

    if speed < target:
        while speed <= target:
            print("Speed is <= target")
            band_start, band_end, acc_low, acc_high, acceleration = 0, 0, 0, 0, 0

            for each in acc_bands:
                band_start, band_end, acc_low, acc_high = each
                if current_speed in range(band_start, band_end + 1):
                    print("Acceleration band is " + str(acc_low) + " " + str(acc_high))
                    acceleration = random.randint(acc_low, acc_high)

            speed += acceleration

            simulated_run = np.append(simulated_run, max(speed, 0))
    else:
        while speed >= target:
            print("Speed is >= target")
            band_start, band_end, acc_low, acc_high, acceleration = 0, 0, 0, 0, 0

            for each in acc_bands:
                band_start, band_end, acc_low, acc_high = each
                if current_speed in range(band_start, band_end + 1):
                    print("Deceleration band is " + str(acc_low) + " " + str(acc_high))
                    acceleration = random.randint(acc_low, acc_high)

            speed -= acceleration

            simulated_run = np.append(simulated_run, max(speed, 0))
    yield simulated_run


# simulated = np.array([])

# for i in range(0, 10, 1):
#     current_speed = 0 if len(simulated) == 0 else simulated[-1]
#     print(current_speed)

#     generated_value = next(generator(current_speed))
#     simulated = np.append(simulated, generated_value)
#     # print(generated_value)
#     # print(simulated)
# print(simulated)


# plt.plot(simulated)
# plt.grid()
# plt.show()
