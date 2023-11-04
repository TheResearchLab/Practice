import pandas as pd 
url = 'https://github.com/mattharrison/datasets/raw/master/data/alta-noaa-1980-2019.csv'
alta_df = pd.read_csv(url)
dates = pd.to_datetime(alta_df.DATE)

snow = (alta_df
        .SNOW 
        .rename(dates)        
)

# Chapter 13.1 Finding Missing Data

# print(
#     snow.isna().any(),
#     snow[snow.isna()],
#     snow.loc['1985-09':'1985-09-20']
# )


# Chapter 13.2 Filling In Missing Data
# print(
#     snow
#         .loc['1987-12-30':'1988-01-10'],
#     snow
#         .loc['1987-12-30':'1988-01-10']    
#         .ffill(),
#     snow
#         .loc['1987-12-30':'1988-01-10']    
#         .bfill()
# )

# Chapter 13.3 Interpolation 
winter = (snow.index.quarter ==1) | (snow.index.quarter==4)

# print(
#     snow
#         .loc['1987-12-30':'1988-01-10']
#         .interpolate(),
#     snow 
#         .where(~(winter&snow.isna()), snow.interpolate())
#         .where(~(~winter&snow.isna()),0)
# )


# Chapter 13.4 Dropping Missing Values
# print(
#     snow
#         .loc['1987-12-30':'1988-01-10']
#         .dropna()
# )

# Chapter 13.5 Shifting data
print(
    snow.shift(1),
    snow.shift(-1)
)