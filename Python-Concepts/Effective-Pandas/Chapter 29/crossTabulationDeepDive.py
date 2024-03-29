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

# Chapter 29.1 Cross-Tab Summaries
pd.crosstab(index=df2.country_live,columns=df2.age)

# Chapter 29.2 Adding Margins
pd.crosstab(index=df2.country_live,columns=df2.age,margins=True)

# Chapter 29.3 Normalizing Results
pd.crosstab(index=df2.country_live,columns=df2.age,normalize=True)
pd.crosstab(index=df2.country_live,columns=df2.age,normalize='columns') # normalize down column
pd.crosstab(index=df2.country_live,columns=df2.age,normalize='index') # normalize across rows sum to 1

# Chapter 29.4 Hierarchial Columns with Cross Tabs
(pd.crosstab(index=[df2.country_live,df2.age],
            columns=[df2.use_python_most,df2.python3_version_most])
            .loc[['United States']])

(pd.crosstab(index=[df2.country_live,df2.age],
            columns=[df2.use_python_most,df2.python3_version_most])
            .loc[['United States'],['Data analysis','Web development']])

# Chapter 29.5 Heat Maps
(pd.crosstab(index=[df2.country_live,df2.age],columns=[df2.use_python_most,df2.python3_version_most])
    .loc[['United States'],['Data analysis','Web development']]
    .style.background_gradient(cmap='viridis',axis=None))
# %%


# %%
