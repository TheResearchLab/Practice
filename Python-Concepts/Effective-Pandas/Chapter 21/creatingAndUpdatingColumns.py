#%%
import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/2020-jetbrains-python-survey.csv'
df = pd.read_csv(url)

df = df.loc[:,~df.columns.duplicated(keep='first')]


import collections

counter = collections.defaultdict(list)

for col in sorted(df.columns):
    period_count = col.count('. ')
    part_end = 2 if period_count >= 2 else 1
    parts = col.split('. ')[:part_end]
    counter['. '.join(parts)].append(col)

uniq_cols = [cols[0] for cols in counter.values() if len(cols) == 1]

for cols in counter.values():
    if len(cols) == 1:
        uniq_cols.extend(cols)
 
# (df
#     [uniq_cols]
#     .rename(columns=lambda c: c.replace('.','_'))
#     ['age']
#     .iloc[:,1:] 
#     .value_counts(dropna=False)
# )

# (df 
#     [uniq_cols]
#     .rename(columns=lambda c: c.replace('.','_'))
#     ['age']
#     .iloc[:,1:]
#     ['age']
#     .str.slice(0,2)
#     .astype(float)

# )

# lmao his bug example was fixed
# (df
#     [uniq_cols]
#     .rename(columns=lambda c:c.replace('.','_'))
#     ['age']
#     .iloc[:,1:]
#     ['age']
#     .str.slice(0,2)
#     .astype('Int64')
# )

# 21.2 More Column Cleanup

import numpy as np
import catboost as cb


# A lot of .assign based cleanup
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

# still have team_size rows with missing values
(df2
 .query('team_size.isna()')
 ['employment_status']
 .value_counts(dropna=False)

)

#use catboost to fill missing values

def prep_for_ml(df):
    return (
        df
        .assign(**{col: df[col].astype(float) for col in df.select_dtypes('number')},
                **{col: df[col].astype(str).fillna(' ') for col in df.select_dtypes(['object', 'category'])})
    )

def predict_col(df, col):
    df = prep_for_ml(df)
    missing = df.query(f'~{col}.isna()')
    cat_idx = [i for i, typ in enumerate(df.drop(columns=[col]).dtypes) if str(typ) == 'object']
    X = missing.drop(columns=[col]).values
    y = missing[col]
    model = cb.CatBoostRegressor(iterations=20, cat_features=cat_idx)
    model.fit (X ,y,cat_features= cat_idx)
    pred = model.predict(df.drop(columns=[col]))
    return df[col].where(~df[col].isna(), pred) 

df2 = df2.assign(team_size=lambda df_:predict_col(df_,'team_size').astype('int'))
df2['team_size'].info()
# %%
