#%%
import pandas as pd

url = 'https://raw.githubusercontent.com/mattharrison/datasets/master/data/2020-jetbrains-python-survey.csv'
jb = pd.read_csv(url)

# Chapter 26.1 Dummy Colums

jb.filter(like='job.role') # get all columns like

(jb
    .filter(like='job.role.*t') # collapse values into single column
    .where(jb.isna(),1)
    .fillna(0)    
) 

job = (jb
    .filter(like='job.role') # collapse values into single column
    .where(jb.isna(),1)
    .fillna(0)
    .idxmax(axis='columns')  # finds the maximum value depending on context, in this case column
    .str.replace('job.role.','',regex=False)
) 

job

dum = pd.get_dummies(job,dtype=int)
dum

# %%
