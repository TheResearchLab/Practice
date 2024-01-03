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

fig,ax= plt.subplots(dpi=600)
# dd.cfs.plot()

# dd.cfs.describe()

# Chapter 31.4 Slicing Time Series
(dd
   .cfs 
   .loc['2018':])

(dd
   .cfs
   .loc['2018/03':'2019/05']
   .clip(upper=400)
   .plot())

dd2018 = (dd 
            .cfs 
            .loc['2018/3':'2019/5']
            .clip(upper=400))

ax = (dd2018 
            .resample('D')
            .mean()
            .plot(figsize=(10,4),alpha=.5,linewidth=1,label='Daily')
            )

ax = (dd2018
            .resample('D')
            .mean()
            .rolling(7)
            .mean()
            .plot(figsize=(10,4),ax=ax,label='7-day Rolling'))

ax.legend()
ax.set_title('Dirty Devil Flow 2018 (cfs)')
# %%
