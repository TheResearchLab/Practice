#%%
import pandas as pd

url = 'https://raw.githubusercontent.com/mattharrison/datasets/master/data/2020-jetbrains-python-survey.csv'
jb = pd.read_csv(url)

# Chapter 26.1 Dummy Colums

# jb.filter(like='job.role') # get all columns like

# (jb
#     .filter(like='job.role.*t') # collapse values into single column
#     .where(jb.isna(),1)
#     .fillna(0)    
# ) 

job = (jb
    .filter(like='job.role') # collapse values into single column
    .where(jb.isna(),1)
    .fillna(0)
    .idxmax(axis='columns')  # finds the maximum value depending on context, in this case column
    .str.replace('job.role.','',regex=False)
) 

job

dum = pd.get_dummies(job,dtype=int)
# dum

# Chapter 26.2 Undoing Dummy Columns

import numpy as np
dum.idxmax(axis='columns') # easiest read method

i,j = np.where(dum)
pd.Series(dum.columns[j],i) # this method is 8x faster than idxmax

# Chapter 26.4 Exercises

# Set seed for reproducibility
np.random.seed(42)

# Define categorical data
categories = ['A', 'B', 'C', 'D']

# Create a DataFrame with categorical columns and some null values
data = {
    'Category1': np.random.choice(categories + [np.nan], size=10),
    'Category2': np.random.choice(categories + [np.nan], size=10),
    'NumericColumn': np.random.randint(1, 100, size=10)
}

df = pd.DataFrame(data)
df.replace('nan',np.nan,inplace=True)

df1 = (df
  .filter(like='Category')
  #.replace('nan',np.nan)
  .where(df.isna(),1)
  .replace(np.nan,0)
)

df2 = df1.idxmax(axis='columns') # remove dummy columns

pd.get_dummies(df2,dtype=int) # create dummy columns



# %%
