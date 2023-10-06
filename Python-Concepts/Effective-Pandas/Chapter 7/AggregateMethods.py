import pandas as pd

df = pd.read_csv(r'vehicles.csv',index_col=None)

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
