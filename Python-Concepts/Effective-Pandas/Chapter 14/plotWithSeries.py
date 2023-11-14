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
# (
#     snow
#         [lambda s:(s.index.month ==1) & (s > 0)]
#         .plot.kde()

# )

# Chapter 14.6 Line Plots 
# snow.plot.line()
# (
#     snow
#         .iloc[-300:]
#         .plot.line()
# )

# Chapter 14.7 Line plots with multiple aggregations
# (snow
#     .resample('Q')
#     .quantile([.5,.9,.99])
#     .unstack() #converts to dataframe
#     .iloc[-300:]
#     .plot.line()
# )

# Chapter 14.8 Bar Plots
season2017 = (snow.loc['2016-10':'2017-05']) 
# (season2017
#         .resample('M')
#         .sum()
#         .div(season2017.sum())
#         .mul(100)
#         .rename(lambda idx: idx.month_name())
#         .plot.bar(title='2017 Monthly Percent of Snowfall')        
# )

(season2017
        .resample('M')
        .sum()
        .div(season2017.sum())
        .mul(100)
        .rename(lambda idx: idx.month_name())
        .plot.barh(title='2017 Monthly Percent of Snowfall')        
)

# %%

#Chapter 14.9 

%matplotlib inline 

import pandas as pd
url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url) 

make = df.make

top10  = make.value_counts().index[:10]

# (make
#     .where(make.isin(top10))
#     .value_counts()
#     .plot.barh()
# )

# Chapter 14.12 Exercises
data = pd.Series(data=[11,12,14,16,20,22,15,33,29,42,21,19])
categorical_data = pd.Series(['blue','black','green','red','red','gray','blue','green','purple','blue','gray'],dtype='category')
#favorite_colors = cat

print(
    #data.plot.hist(bins=4),
    #data.plot.box(),
    #data.plot.kde(),
    #data.plot.line(),
    #categorical_data.value_counts().plot.barh(),
    categorical_data.value_counts().plot.pie(title='Students favorite colors')
)


# %%
