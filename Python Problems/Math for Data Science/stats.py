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
