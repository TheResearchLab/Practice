import pandas as pd 
url = 'https://github.com/mattharrison/datasets/raw/master/data/alta-noaa-1980-2019.csv'
alta_df = pd.read_csv(url)
dates = pd.to_datetime(alta_df.DATE)

snow = (alta_df
        .SNOW 
        .rename(dates)        
)

# Chapter 13.1 Finding Missing Data

print(
    snow.isna().any(),
    snow[snow.isna()],
    snow.loc['1985-09':'1985-09-20']
)