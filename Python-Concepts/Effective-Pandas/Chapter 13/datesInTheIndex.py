import pandas as pd 
import numpy as np 

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
# print(
#     snow.shift(1),
#     snow.shift(-1)
# )

# Chapter 13.6 Rolling Averages
# print(
#     snow
#         .rolling(5)
#         .mean(),
#     snow
#         .loc['1987-12-30':'1988-01-10']
#         .rolling(3)
#         .std()
# )

# Chapter 13.7 Resampling
# print(snow
#           .resample('M') #aggregate records at end of month
#           .max(),
#       snow 
#           .resample('2M') # end every two months
#           .max(),
#       snow
#           .resample('A-MAY') # end annual in may
#           .max()
# )

# Chapter 13.8 Gathering Aggregate Values (But Keeping Index)
season2017 = snow.loc['2016-10':'2017-05']

# print(
#     snow
#         .div(snow   
#                  .resample('Q')
#                  .transform('sum')) #transform returns a series with the original index
#         .mul(100)
#         .fillna(0),
#     season2017
#               .resample('M')
#               .sum()
#               .div(season2017
#                         .sum())
#               .mul(100)
# )

# Chapter 13.9 Groupby Operations
def season(idx):
    year = idx.year
    month = idx.month
    return np.where((month<10),year,year+1) #year.where doesn't work but numpy does


print(snow
           .groupby(season)
           .sum(),
      snow
          .resample('A-SEP')
          .sum()
 )

 