#%%
import pandas as pd

# Chapter 16.3 Dataframes
df = pd.DataFrame({'growth':[.5,.7,1.2],
                    'Name':['Paul','Gary','John']})

print(
    df,
    df.iloc[2],
    df['Name'],
    type(df['Name']), #pandas series
    df['Name'].str.lower()
)



# %%
