#%%

import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/siena2018-pres.csv'
df = pd.read_csv(url, index_col=0)


#df.select_dtypes('int64')
def tweak_siena_pres(df):
    def int64_to_uint8(df_):
        cols = df_.select_dtypes('int64')
        return df_.astype({col: 'uint8' for col in cols})

    return (
        df
        .rename(columns={'Seq.': 'Seq'})  # 1
        .rename(columns={k: v.replace(' ', '_') for k, v in
                         {'Bg': 'Background',
                          'PL': 'Party_leadership',
                          'CAb': 'Communication_ability',
                          'RC': 'Relations_with_Congress',
                          'CAp': 'Court_appointments',
                          'HE': 'Handling_of_economy',
                          'L': 'Luck',
                          'AC': 'Ability_to_compromise',
                          'WR': 'Willing_to_take_risks',
                          'EAp': 'Executive_appointments',
                          'OA': 'Overall_ability',
                          'Im': 'Imagination',
                          'DA': 'Domestic_accomplishments',
                          'Int': 'Integrity',
                          'EAb': 'Executive_ability',
                          'FPA': 'Foreign_policy_accomplishments',
                          'LA': 'Leadership_ability',
                          'IQ': 'Intelligence',
                          'AM': 'Avoid_crucial_mistakes',
                          'EV': "Experts_view",
                          'O': 'Overall'}.items()})
        .astype({'Party': 'category'})  # 2
        .pipe(int64_to_uint8)  # 3
        .assign(Average_rank=lambda df_: (df_.select_dtypes('uint8')  # 4
                                          .sum(axis=1)
                                          .rank(method='dense')
                                          .astype('uint8')),
                Quartile=lambda df_: pd.qcut(df_.Average_rank, 4,
                                             labels='1st 2nd 3rd 4th'.split())
                )
    ) 

pres = tweak_siena_pres(df)

# Chapter 18.1 Index Alignment
scores = (pres
    .loc[:,'Background':'Average_rank']
)

s1 = scores.iloc[:3,:4] #get three rows and 4 columns
s2 = scores.iloc[1:6, :5] #get 5 rows from position 2 to 6 (inclusive) rows and first 5 columns

#s1
#s2
s1 + s2 #only index 2 & 3  

# Chapter 18.2 Duplicate Index Entries
scores.iloc[:3,:4] + pd.concat([scores.iloc[1:6,:5]]*2)

# can check for duplicated values with 
pd.concat([scores.iloc[1:6,:5]]*2).index.duplicated().any()


# Chapter 18.3 Exercises 

data = {'column 1':[i for i in range(0,10)],
        'column 2':[i for i in range(0,20,2)]}

df = pd.DataFrame(data)
add_df = df + df

multiply_by_two_df = df*2
multiply_by_two_df


# %%
