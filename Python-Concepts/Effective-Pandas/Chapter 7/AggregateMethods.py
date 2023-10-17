import pandas as pd
import numpy as np

df = pd.read_csv(r'vehicles.csv',index_col=None)

# 7.1
city_mpg = df.city08
highway_mpg = df.highway08

print(city_mpg.mean())

# is properties
print(city_mpg.is_unique)
print(highway_mpg.is_monotonic_increasing)

# quantile return 50% by default
print(city_mpg.quantile)
print(city_mpg.quantile(.95))
print(highway_mpg.quantile([0.1,0.8,0.99]))

# 7.2

# sum w/ criteria
mpg_sum = (city_mpg
                .gt(20)
                .sum())

# pct of values that meet criteria

mpg_mean = (city_mpg
                .gt(20)
                .mul(100)
                .mean())                

print(mpg_sum)
print(mpg_mean) #works because math operations work on booleans

# 7.3 .agg and aggregation strings
print(city_mpg.agg('mean'))

def second_to_last(s):
    return s.iloc[-2]

print(city_mpg.agg(['mean',np.var,max,second_to_last]))

