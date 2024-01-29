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
    length = len(lst)
    # if the data is odd
    if length % 2 != 0:
        cnts = int((length -1) / 2)
        print(cnts)
        q1_index = lst[:cnts]
        q3_index = lst[cnts+1:] 

    return q1_index,q3_index
    # if the data is even

print(calc_iqr([1,2,3,4,5,6,7,8,9]))
# %%
