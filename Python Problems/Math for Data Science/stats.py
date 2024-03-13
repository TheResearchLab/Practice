#%%
from statistics import mean
sorted([329,479,459,269,369,799,649])


def calc_stdev(lst:list) -> float:
    denominator = len(lst) - 1
    mean_val = mean(lst)
    lst_minus_mean = map(lambda x: (x-mean_val)**2,lst)
    return sum(lst_minus_mean)/denominator


calc_stdev([220.1,220.0,220.0,220.2,220.1]) # good
calc_stdev([220.1,220.4,220.2,220.0,220.1]) # not good

def find_median(lst:list) -> float:
    length = len(lst)
    if length % 2 == 1: # odds
        return lst[length//2]
        
    else: # even 
        mid_left = lst[length//2-1]
        mid_right = lst[length//2]
        return mean([mid_left,mid_right])




def calc_iqr(lst):
    lst = sorted(lst)
    
    def find_median(lst):
        length = len(lst)
        if length % 2 == 1: # odds
            return lst[length//2]
        
        else: # even 
            mid_left = lst[length//2-1]
            mid_right = lst[length//2]
            return mean([mid_left,mid_right])

            
    
    length = len(lst)
    
    # if the data is odd
    if length % 2 == 1:
        cnts = (length -1) // 2
        q1 = lst[:cnts]
        q3 = lst[cnts+1:]

    # if the data is even
    else:
        q1_index = len(lst)//2-1
        q3_index = len(lst)//2
        q1 = lst[:q1_index]
        q3 = lst[q3_index+1:]
        


    return find_median(q1),find_median(q3),find_median(lst)
    


arr = [1,2,3,4,5]
print(calc_iqr(arr))

# print(arr[len(arr)//2-1])
# print(arr[len(arr)//2])



# %%

import random

# Define the list of elements (population)
population = list(range(1, 101))

# Define the number of clusters and cluster size
num_clusters = 3
cluster_size = 10

# Randomly select clusters
selected_clusters = random.sample(population, num_clusters)

# Sample individuals within each selected cluster
sampled_data = []
for cluster in selected_clusters:
    cluster_data = random.sample(range(cluster, cluster + cluster_size), cluster_size)
    sampled_data.extend(cluster_data)

print("Sampled data:")
print(sampled_data)


# %%
import numpy as np

data = np.random.normal(loc=0, scale=1, size=1000)
mean, std_dev = np.mean(data), np.std(data)
within_one_std_dev = np.mean(np.abs(data - mean) < std_dev)
within_two_std_dev = np.mean(np.abs(data - mean) < 2 * std_dev)
within_three_std_dev = np.mean(np.abs(data - mean) < 3 * std_dev)

print("Within 1 std dev:", within_one_std_dev * 100)
print("Within 2 std devs:", within_two_std_dev * 100)
print("Within 3 std devs:", within_three_std_dev * 100)

# %%
import numpy as np
import matplotlib.pyplot as plt

# Generate random data with a normal distribution
data = np.random.normal(loc=0, scale=1, size=1000)

# Calculate mean and standard deviation
mean = np.mean(data)
std_dev = np.std(data)

# Create a range of values for x-axis
x = np.linspace(mean - 4 * std_dev, mean + 4 * std_dev, 1000)

# Create the normal distribution curve
y = (1 / (np.sqrt(2 * np.pi) * std_dev)) * np.exp(-0.5 * ((x - mean) / std_dev) ** 2)

# Plot the normal distribution curve
plt.plot(x, y, color='blue', label='Normal Distribution')

# Shade the area within one standard deviation of the mean
plt.fill_between(x, y, where=(x >= mean - std_dev) & (x <= mean + std_dev), color='green', alpha=0.3, label='Within 1 Std Dev')

# Shade the area within two standard deviations of the mean
plt.fill_between(x, y, where=(x >= mean - 2 * std_dev) & (x <= mean + 2 * std_dev), color='yellow', alpha=0.3, label='Within 2 Std Dev')

# Shade the area within three standard deviations of the mean
plt.fill_between(x, y, where=(x >= mean - 3 * std_dev) & (x <= mean + 3 * std_dev), color='red', alpha=0.3, label='Within 3 Std Dev')

# Add labels for the percentages
plt.text(mean + 0.5 * std_dev, 0.25, '68%', color='green')
plt.text(mean + 1.5 * std_dev, 0.05, '95%', color='yellow')
plt.text(mean + 2.5 * std_dev, 0.005, '99.7%', color='red')

# Add legend and labels
plt.legend()
plt.xlabel('Data')
plt.ylabel('Probability Density')
plt.title('Empirical Rule')

# Show the plot
plt.show()


# %%


# %%

def get_mean(lst:list) -> float:
    return sum(lst)/len(lst)

nums = [1,2,3,4,5,5,6,7,100]
get_mean(nums)
# %%

import numpy as np
import matplotlib.pyplot as plt

# Generate sample data
x = np.random.rand(100)
y = 2 * x + np.random.normal(0, 0.1, 100)

# Calculate Pearson correlation coefficient
corr_coef = np.corrcoef(x, y)[0, 1]

# Plot the data
plt.scatter(x, y)
plt.title('Scatter Plot of X and Y')
plt.xlabel('X')
plt.ylabel('Y')
plt.text(0.1, 2, f'Pearson Correlation: {corr_coef:.2f}', fontsize=12, color='red')
plt.grid(True)
plt.show()
# %%

import numpy as np
import matplotlib.pyplot as plt
import math

def student_t_pdf(t, nu):
    numerator = math.gamma((nu + 1) / 2)
    denominator = math.sqrt(nu * math.pi) * math.gamma(nu / 2)
    term = (1 + (t ** 2) / nu) ** (-(nu + 1) / 2)
    return numerator / denominator * term

# Define range of t values
t_values = np.linspace(-5, 5, 1000)

# Degrees of freedom parameter
nu = 5

# Calculate PDF values for each t value
pdf_values = [student_t_pdf(t, nu) for t in t_values]

# Plot the PDF
plt.figure(figsize=(15, 6))
plt.plot(t_values, pdf_values, label=f'Student\'s t-distribution (ν={nu})')
plt.title('PDF of Student\'s t-distribution')
plt.xlabel('t')
plt.ylabel('Probability Density')
plt.legend()
plt.grid(True)
plt.show()

# %%
