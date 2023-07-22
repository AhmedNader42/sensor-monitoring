import numpy as np
import matplotlib.pyplot as plt
import random


def generate_normal_distribution_with_corruption(mean, variance):
    generated_distribution = np.random.normal(mean, variance, 1000)

    generated_distribution_with_corruption = []
    for ind in range(0, len(generated_distribution), 1):
        corrupt_probability = random.random()
        if corrupt_probability < 0.2:
            generated_distribution_with_corruption.append("!~@! Corrupted Reading !@~!")
        else:
            generated_distribution_with_corruption.append(
                str(generated_distribution[ind])
            )

    print(generated_distribution_with_corruption)
    return generated_distribution_with_corruption, generated_distribution


mu, sigma = 70, 10
generated_clean, generated_corrupt = generate_normal_distribution_with_corruption(
    mu, sigma
)

count, bins, ignored = plt.hist(generated_clean, 30, density=True)
plt.plot(
    bins,
    1 / (sigma * np.sqrt(2 * np.pi)) * np.exp(-((bins - mu) ** 2) / (2 * sigma**2)),
    linewidth=2,
    color="r",
)

plt.show()
