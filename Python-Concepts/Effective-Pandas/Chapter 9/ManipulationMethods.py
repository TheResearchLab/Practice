import pandas as pd
import numpy as np
import time as t

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)
city_mpg = df['city08']
highway_mpg = df['highway08']

# Chapter 9.1 .apply and .where
print('===============Section 1================')
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

# Chapter 9.2 If Else with Pandas
print('===============Section 2================')
top_10 = make.value_counts()[:10]
print(top_10)

def generalize(val):
    if val in top_5:
        return val
    if val in top_10:
        return 'top10'
    return 'other'

#print(make.apply(generalize).value_counts())

print( #this is incorrect
(make.where(make.isin(top_5),'top_10')
     .where(make.isin(top_10),'other')
     .value_counts())
)

#numpy method
#logic is also behaving unexpectedly
print(pd.Series(np.select([make.isin(top_5),make.isin(top_10)],[make,'Top10'],'Other'),index=make.index).value_counts())

# Chapter 9.3 Missing Data
print('===============Section 3================')
cyl = df.cylinders

print((cyl  
        .isna()
        .sum()
))

missing = cyl.isna()
print(make.loc[missing]) #.loc for boolean indexing 
