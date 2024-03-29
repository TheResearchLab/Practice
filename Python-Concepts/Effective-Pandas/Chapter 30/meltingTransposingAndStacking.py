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



scores = pd.DataFrame({
    'name': ['Adam', 'Bob', 'Dave', 'Fred'],
    'age': [15, 16, 16, 15],
    'test1': [95, 81, 89, None],
    'test2': [80, 82, 84, 88],
    'teacher': ['Ashby', 'Ashby', 'Jones', 'Jones']
})

scores.melt(id_vars=['name','age'],
            value_vars=['test1','test2'])

scores.melt(id_vars=['name','age'],
            value_vars=['test1','test2'],
            var_name='test',value_name='scores')

# save the teacher column too
scores.melt(id_vars=['name','age','teacher'],
            value_vars=['test1','test2'],
            var_name='test',value_name='scores')

# Chapter 30.2 Un-melting Data
melted = scores.melt(id_vars=['name','age','teacher'],
            value_vars=['test1','test2'],
            var_name='test',value_name='scores')

(melted 
    .pivot_table(index=['name','age','teacher'],columns='test',values='scores')) # unflattened

(melted 
    .pivot_table(index=['name','age','teacher'],columns='test',values='scores')).reset_index() # flattened

# unmelting melted data
(melted
    .groupby(['name','age','teacher','test'])
    .scores
    .mean() 
    .unstack()
    .reset_index())


# Chapter 30.3 transposing data
(df2
    .head(10)
    .T)

# Chapter 30.4 Stacking and Unstacking
# (df2
#     .groupby(['country_live','are_you_datascientist'])
#     .size() # type error '<' not supported between instances of str and bool
#     )

# Chapter 30.5 Stacking

# age and company size in index
(df2
    .pivot_table(index='country_live',
                 aggfunc={'age':['min','max'],
                          'company_size':['min','max']})
    .stack(1)
)

# min max in index
(df2
    .pivot_table(index='country_live',
                 aggfunc={'age':['min','max'],
                          'company_size':['min','max']})
    .stack(0)
)

(df2
    .pivot_table(index='country_live',
                 aggfunc={'age':['min','max'],
                          'company_size':['min','max']})
    .stack(1)
    .swaplevel()
)

# Chapter 30.6 Flattenting Hierarchial Indexes and Columns
(df2
    .groupby(['country_live','age'])
    .agg('mean'))

df3 = df2.set_index(['country_live','age'])
df3.groupby(level=0,observed=True).mean()

## Section has outdated code or something
# %%
