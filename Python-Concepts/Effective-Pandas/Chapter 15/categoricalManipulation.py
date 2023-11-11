#%%


import pandas as pd

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)

make = df.make

# Chapter 15.2 Frequency counts
#print(
#     make.value_counts(),
#     make.shape,
#    make.nunique()
# )

# Chapter 15.3  Benefits of Categories

cat_make = make.astype('category')
# print(
#     make.memory_usage(deep=True),
#     cat_make.memory_usage(deep=True) #lower memory usage
# )

import timeit 
code = """cat_make.str.upper()"""
execution_time = timeit.timeit(stmt=lambda: eval(code),number=1)
print(f'execution time: {execution_time}')

code = """make.str.upper()""" #make is slower
execution_time = timeit.timeit(stmt=lambda: eval(code),number=1)
print(f'execution time: {execution_time}')





# %%


