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

# Chapter 31.2 Adding Timezone Information
dd.tz_cd

def to_america_denver_time(df_,time_col,tz_col):
    return(df_  
              .assign(**{tz_col:df[tz_col].replace('MDT','MST7MDT')})
              .groupby(tz_col)
              [time_col]
              .transform(lambda s: pd.datetime(s)
                                        .dt.tz_localize(s.name,ambiguous=True)
                                        .dt.tz_convert('America/Denver'))                        
              
    )

# Chapter 31.3 Exploring the Data
import matplotlib.pyplot as plt 

# fig,ax= plt.subplots(dpi=600)
# dd.cfs.plot()

# dd.cfs.describe()

# Chapter 31.4 Slicing Time Series
# (dd
#    .cfs 
#    .loc['2018':])

# (dd
#    .cfs
#    .loc['2018/03':'2019/05']
#    .clip(upper=400)
#    .plot())

# dd2018 = (dd 
#             .cfs 
#             .loc['2018/3':'2019/5']
#             .clip(upper=400))

# ax = (dd2018 
#             .resample('D')
#             .mean()
#             .plot(figsize=(10,4),alpha=.5,linewidth=1,label='Daily')
#             )

# ax = (dd2018
#             .resample('D')
#             .mean()
#             .rolling(7)
#             .mean()
#             .plot(figsize=(10,4),ax=ax,label='7-day Rolling'))

# ax.legend()
# ax.set_title('Dirty Devil Flow 2018 (cfs)')

# Chapter 31.5 Missing Timeseries Data


# (dd
#     [['cfs']]
#     .loc['2018/3':'2019/5']
#     .query('cfs.isna()')) # can call isna from within query

# fig,ax = plt.subplots(dpi=600,figsize=(10,4))
# dd_july = (dd
#     [['cfs']]
#     .loc['2018/7/7 11:00' :'2018/7/8 20:00']
# )

# dd_july.plot(ax=ax, label='original',linewidth=2)
# (dd_july
#         .bfill()
#         .add(.05)
#         .plot(label='bfill',ax=ax, linewidth=.5))

# (dd_july
#         .ffill()
#         .add(.1)
#         .plot(label='ffill',ax=ax,linewidth=.5))

# (dd_july
#         .interpolate(method='polynomial',order=3)
#         .add(.15)
#         .plot(ax=ax,linewidth=.5))

# (dd_july
#         .interpolate()
#         .add(.2)
#         .plot(ax=ax,linewidth=.5))

# (dd_july
#         .interpolate(method='nearest')
#         .add(.25)
#         .plot(ax=ax,linewidth=.5))

# (dd_july
#         .fillna(1)
#         .add(.3)
#         .plot(ax=ax,linewidth=.5))

# ax.legend()
# ax.set_title('Filling Missing Data Demo')

# 31.6 Exploring Seasonality
# (dd
#     .groupby(dd.index.month)
#     .cfs
#     .describe())

# fig,ax = plt.subplots(dpi=600,figsize=(10,4))
# (dd
#     .groupby(dd.index.month)
#     ['cfs']
#     .describe()
#     ['mean']
#     .plot.bar(ax=ax))

# fig,ax = plt.subplots(dpi=600,figsize=(10,4))
# (dd 
#     .groupby(dd.index.month)
#     ['cfs']
#     .describe()
#     .loc[:,'min':'75%']
#     .plot.bar(ax=ax))

import seaborn as sns

dd = dd.loc[dd.index.drop_duplicates(keep=False)]

fig, ax = plt.subplots(dpi=600, figsize=(10, 4))
sns.boxplot(data=dd.assign(cfs=dd.cfs.clip(upper=400)),
            x=dd.index.month.rename('Month'), y='cfs', ax=ax)


# %%
