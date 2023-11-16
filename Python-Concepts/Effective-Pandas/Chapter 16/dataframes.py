#%%
import pandas as pd

# Chapter 16.3 Dataframes
df = pd.DataFrame({'growth':[.5,.7,1.2],
                    'name':['Paul','Gary','John']})

# print(
#     df,
#     df.iloc[2],
#     df['Name'],
#     type(df['Name']), #pandas series
#     df['Name'].str.lower()
# )

# Chapter 16.4 Construction
import numpy as np 

# numpy array to df
#pd.DataFrame(np.random.randn(10,3),columns=['a','b','c'])

#Chapter 16.5 Dataframe Axis

df.axes 
df.sum(axis=0)
df.apply(pd.to_numeric, errors='coerce').sum(axis='columns') #unsupported operand type(s)
df =  df.apply(pd.to_numeric, errors='coerce')
df.apply(np.sum, axis=1) # axis 1 = column axis
df.apply(np.sum, axis=0) # axis 2 = index axis

# Chapter 16.7 Exercises 

friends = {'name':['jake','liz','mads'],
           'age':[29,27,27],
           'title':['park guide','house ceo','damsel in distress']}

friends_df = pd.DataFrame(friends)

print(
    #friends_df,
    #friends_df['name'].str.upper(),
    friends_df['name'].sum() #concat the values together
      
)
# %%
