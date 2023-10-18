import pandas as pd
import numpy as np

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)
city_mpg = df['city08']
highway_mpg = df['highway08']

#Chapter 8.1
print('===============Section 1================')

# from dtype int64 to Int64
print(city_mpg)
print(city_mpg.convert_dtypes())

#astype method
print(city_mpg.astype('Int16'))
# print(city_mpg.astype('Int8')) # causes error. Signed types too low

# Can save on memory with types if using the appropriate type for the need
print(np.iinfo('int64')) #inspect limits on int & float types


# Chapter 8.2
print('===============Section 2================')
print(city_mpg.nbytes)
print(city_mpg.astype('int16').nbytes)

make = df.make 
print(make.nbytes) #only shows memory of pandas object (ancillary parts)
print(make.memory_usage()) #this includes pandas object plus index memory
print(make.memory_usage(deep=True)) # includes string objects themselves


# Chapter 8.3 
print('===============Section 2================')
print(city_mpg.astype('category'))
print(city_mpg.astype(str))

# order your own categorical data type

sorted_values = pd.Series(sorted(set(city_mpg)))
city_type = pd.CategoricalDtype(categories=sorted_values,ordered=True)
print(city_mpg.astype(city_type))
