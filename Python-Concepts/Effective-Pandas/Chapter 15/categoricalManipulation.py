#%%
import pandas as pd

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)

make = df.make

# Chapter 15.2 Frequency counts
print(
    make.value_counts(),
    make.shape,
    make.nunique()

)

# %%
