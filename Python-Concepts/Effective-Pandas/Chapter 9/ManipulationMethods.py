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

# Chapter 9.4 Filling In Missing Data
print('===============Section 4================')

print(cyl.fillna(0).loc[7136:7141])
print(cyl.ffill())
print(cyl.bfill())
print(cyl.fillna(cyl.mean()))

# Chapter 9.5 Interpolating Data
print('===============Section 5================')
temp = pd.Series([32,40,None,42,39,32])
print(temp,temp.interpolate()) # index 3 becomes 41 based on the surrounding values

# Chapter 9.6 Clipping Data
print('===============Section 6================')
print((
    city_mpg.loc[:446]
            .clip(lower=city_mpg.quantile(.05),
                  upper=city_mpg.quantile(.95))
)) #results are clipped between 5th and 95th percentile

# Chapter 9.7 Sorting Values
print('===============Section 7================')
print(city_mpg.sort_values()) 
print((city_mpg.sort_values() + highway_mpg) / 2) # Can still do math because of index alignment

# Chapter 9.8 Sorting the Index
print('===============Section 8================')
print(city_mpg.sort_values().sort_index()) # unsorting and sorting the index

# Chapter 9.9 Dropping Duplicates
print('===============Section 9================')
print(cyl.drop_duplicates())
print(cyl.drop_duplicates(keep="last"))

# Chapter 9.10 Ranking Data
print('===============Section 10================')
print(city_mpg.rank())
print(city_mpg.rank(method='min'))
print(city_mpg.rank(method='dense'))

# Chapter 9.11 Replacing Data
print('===============Section 11================')
print(make.replace('Subaru','スバル'))
print(make.replace(r'(Fer)ra(r.*)',
            value=r'\2-other-\1',regex=True))

s = pd.Series([40,20,30,20,10])
s = s.replace(to_replace=[40,10], value=[42, 9.8])
print(s)
print(s.replace(to_replace={42:56,9.8:23}))

# Chapter 9.12 Binning Data
print('===============Section 12================')
print(pd.cut(city_mpg,10))
print(pd.cut(city_mpg,[0,10,20,40,70,150])) # need N+1 edges for N bins 
print(pd.qcut(city_mpg,10)) #quantile binning

# Chapter 9.13 Exercises
print('===============Section 13================')
s = pd.Series([1,11,111,1111,2,22,222,2222,3,33,333,3333])

def high_low(val):
    return val >= s.mean()

start = t.time()
print(s.apply(high_low).replace(to_replace=[True,False], value=['high','low']))
print(f'apply final time is {t.time() - start}')

start = t.time()
print((
    np.select([s.gt(s.mean())],['high'],default='low')    
))
print(f'np.select final time is {t.time() - start}')

s = pd.Series([1,2,None,3,4,5,6,None,8,9])
print(s.fillna(s.median()))

s = pd.Series([1,2,3,4,5,6,7,8,9])
print(s.clip(lower=s.quantile(.1),upper=s.quantile(.95)))

s = pd.Series(['xbox','ps3','gamecube','ps3','xbox','wii','switch','xbox'],dtype='category')
not_2 = s.value_counts().index[2:]
print(s.replace({key:'other' for key in not_2}))

def get_top_n(s,n):
    not_top_n = s.value_counts().index[n:]
    return s.replace({key:'other' for key in not_top_n})

print(pd.cut(pd.Series([i for i in range(20)]),10))
print(pd.qcut(pd.Series([i for i in range(20)]),10))

