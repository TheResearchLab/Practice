#%%

import pandas as pd
import numpy as np

url = 'https://github.com/mattharrison/datasets/raw/master/data/2020-jetbrains-python-survey.csv'
df = pd.read_csv(url)

df = df.loc[:,~df.columns.duplicated(keep='first')]

df2 = (
    df
    .rename(columns=lambda c: c.replace('.', '_'))
    .assign(age=lambda df_: pd.to_numeric(df_['age'].astype(str).str.slice(0, 2),errors='coerce')
                .astype('Int64'),
            are_you_datascientist=lambda df_:df_['are_you_datascientist']
                .replace({'Yes':True,'No':False,np.nan:False}),
            company_size = lambda df_ : df_['company_size'].replace({'Just me':1 ,
                'Not sure': np.nan , 'More than 5,000': 5000 ,'2–10': 2 , '11–50': 11,'51–500': 51, '501–1,000': 501,
                '1,001–5,000': 1001}).astype('Int64'),
            country_live=lambda df_:df_['country_live'].astype('category'),
            employment_status=lambda df_:df_['employment_status']
                                        .fillna('Other').astype('category'),
            is_python_main=lambda df_:df_['is_python_main'].astype('category'),
            team_size=lambda df_:df_['team_size']
                        .str.split(r'-',n=1,expand=True)
                        .iloc[:,0].replace('More than 40 people',41)
                        .where(df_['company_size']!=1,1).astype(float),
            years_of_coding=lambda df_:df_['years_of_coding']
                        .replace('Less than 1 year',.5).str.extract(r'(\d+)')
                        .astype(float),
            python_years=lambda df_:df_['python_years']
                        .replace('Less than 1 year',.5).str.extract(r'(\d+)')
                        .astype(float),
            python3_ver=lambda df_:df_['python3_version_most']
                        .str.replace('_','.').str.extract(r'(\d\.\d)')
                        .astype(float),
            use_python_most=lambda df_:df_['use_python_most']
                        .fillna('Unknown')
            )      
            .drop(columns=['python2_version_most'])        
)

# Chapter 27.1 A basic Example
# method 1
# (df2
#     .pivot_table(index='country_live',columns='employment_status', values='age',aggfunc='mean'))

# # method 2
# pd.crosstab(index=df2.country_live,columns=df2.employment_status,values=df2['age'],aggfunc='mean')

# # method 3
# (df2
#     .groupby(['country_live','employment_status'])
#     .age
#     .mean()
#     .unstack())

# # Chapter 27.2 Using a Custom Agg Function

# def per_emacs(ser):
#     return ser.str.contains('Emacs').sum() / len(ser) * 100 

# (df2
#     .pivot_table(index='country_live',values='ide_main',aggfunc=per_emacs))

# pd.crosstab(index=df2.country_live,
#             columns=df2.assign(iden='emacs_per').iden,
#             values=df2.ide_main, aggfunc=per_emacs)

# df2.assign(iden='emacs_per').iden

# df2.groupby(['country_live'])[['ide_main']].agg(per_emacs)


# Chapter 27.3 Multiple Aggs


# (df2
#     .pivot_table(index='country_live',values='age',aggfunc=(min,max)))


# (df2.groupby('country_live')[['age']].agg(['max','min']))

# pd.crosstab(df2.country_live, values=df2['age'],aggfunc=(min,max), # need to set age as val to perform agg
#             columns=df2.assign(val='age').val)

# Chapter 27.4 Per Column Aggs - Non Operational Code

# (df2    
#     .pivot_table(index='country_live',aggfunc=(min,max))
# )

# df2['country_live'] = df2['country_live'].astype('category').cat.as_ordered()

# (df2.groupby('country_live').agg(['min']))

# agg_dict = {'age':['min','max'],
#             'team_size':'mean'}

# df2.groupby('country_live').agg(agg_dict) # this works, but returns nasty df with stacked columns

# df2.groupby('country_live').agg(age_min=('age','min'),
#                                 age_max=('age','max'),
#                                 team_size_mean=('team_size','mean'))

# Chapter 27.5 Grouping by Hierarchy

# (df2.pivot_table(index=['country_live','ide_main'],
#                  values='age', aggfunc=['min','max'])).swaplevel(axis='columns')

# (df2.groupby(['country_live','ide_main'],observed=True)[['age']].agg(['min','max']))


# Chapter 27.6 Grouping with Functions

# def even_grouper(idx):
#     return 'odd' if idx % 2 else 'even'

# df2.pivot_table(index=even_grouper, aggfunc='size')

# df2.groupby(even_grouper).size()

# Chapter 27.8 Exercises
import random
from datetime import datetime, timedelta

# Set a random seed for reproducibility
random.seed(42)

# Generate sample data
num_records = 100
products = ['ProductA', 'ProductB', 'ProductC']
categories = ['Category1', 'Category2', 'Category3']

data = {
    'Date': [datetime(2023, 1, 1) + timedelta(days=random.randint(1, 30)) for _ in range(num_records)],
    'Product': [random.choice(products) for _ in range(num_records)],
    'Category': [random.choice(categories) for _ in range(num_records)],
    'Sales': [random.uniform(100, 1000) for _ in range(num_records)],
    'Quantity': [random.randint(1, 10) for _ in range(num_records)],
}

# Create a DataFrame
df = pd.DataFrame(data)

# Mean sales quantity by product category 
df.groupby('Product')['Quantity'].agg('mean')

# Mean sales quantity and max quantity by product category
df.groupby('Product')['Quantity'].agg(['mean','max'])

df.groupby('Product')['Quantity'].size()

def find_mode(idx):
    return pd.Series.mode(df['Product']).iloc[0] if df['Product'].iloc[idx] == pd.Series.mode(df['Product']).iloc[0] else 'Other'

print(find_mode(26))

df.groupby(find_mode)['Quantity'].agg('mean')

df.groupby(['Product','Category'])['Sales'].agg('mean') # returns pandas series

df.groupby(['Product','Category'])[['Sales']].agg('mean') #returns a pandas df


df['Sales'] = df['Sales'].astype(float)
df.groupby(['Product'])['Sales'].quantile(q=.5,interpolation='linear')

def create_quartiles(idx):
    return pd.qcut(idx,q=[0,0.25,0.5,0.75,1])

df['sales_bucket'] = df.groupby('Product')['Sales'].transform(create_quartiles) #transform used instead of apply here because don't run into index issue
df.groupby(['Product','sales_bucket'])['Sales'].agg('mean')
# %%
