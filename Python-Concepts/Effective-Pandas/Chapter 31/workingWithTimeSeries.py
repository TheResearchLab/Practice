#%%

# Chapter 31.1 Loading the data
import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/dirtydevil.txt'

# Read CSV with custom skiprows and separator
df = pd.read_csv(url, skiprows=lambda num: num < 34 or num == 35, sep='\t')

def tweak_river(df_):
    return (
        df_
        .assign(datetime=pd.to_datetime(df_['datetime']))
        .rename(columns={'144166_00060': 'cfs', '144167_00065': 'gage_height'})
        .set_index('datetime')
    )

# Apply the tweak_river function to the DataFrame
dd = tweak_river(df)
dd
# %%
