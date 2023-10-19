import pandas as pd
import numpy as np
import time as t

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)
city_mpg = df['city08']
highway_mpg = df['highway08']

# Chapter 9.1 .apply and .where
def gt20(value):
    return value > 20

start = t.time()
city_mpg.apply(gt20)
apply_time = t.time() - start 

start = t.time()
city_mpg.gt(20)
gt_time = t.time() - start 

print(gt_time,apply_time) #apply is slower

make = df.make 
top_5 = make.value_counts().index[:5]
print(make.value_counts().index[:5])

def get_top5(val):
    if val in top_5:
        return val 
    return 'Other'

print(make.apply(get_top5)) #This is extremely slow

# WHERE is better
print(make.where(make.isin(top_5),other='Other'))

# Same thing using the mask
print(make.mask(~make.isin(top_5),other='Other'))