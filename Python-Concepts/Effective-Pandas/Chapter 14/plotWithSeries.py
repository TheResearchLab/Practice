#%%

%matplotlib inline

import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/alta-noaa-1980-2019.csv'
alta_df = pd.read_csv(url)
dates = pd.to_datetime(alta_df.DATE)

snow = (alta_df
        .SNOW 
        .rename(dates)        
)

# Chapter 14.3 Histogram

#snow[snow>0].plot.hist()
#snow[snow>0].plot.hist(bins=20,title='Snowfall Histogram (in)')

# Chapter 14.4 boxplot

#snow.plot.box()
# (snow
#     [lambda s:(s.index.month == 1) & (s>0)] #Lambda like where clause
#     .plot.box()
# )

# Chapter 14.5 Kener density estimation
# essentially a smoothed histogram
(
    snow
        [lambda s:(s.index.month ==1) & (s > 0)]
        .plot.kde()

)






# %%
