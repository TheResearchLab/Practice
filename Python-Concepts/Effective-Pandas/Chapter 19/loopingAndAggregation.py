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


# Chapter 19.1 For Loops

for key,value in pres.items():
    # print(
    #     f'label: {key}',
    #     f'content:{value}'
    #     )
    break

for idx,row in pres.iterrows():
    #print(f'index:{idx}',type(row))
    break

for tup in pres.itertuples():
    #print(f'index:{tup[0]}',tup.Party)
    break

# Chapter 19.2 Aggregations

scores = (pres  
    .loc[:,'Background':'Average_rank']
    )


scores.sum(axis='columns') / len(scores.columns)

# Aggregate Method to do multiple aggregations
scores.agg(['count','size','sum',lambda col: col.loc[1]])


scores.agg({'Background':['count','size'], 'Integrity':['count','max']})

scores.agg(Intelligence_count=('Intelligence','count'),
            Intelligence_size=('Intelligence','size'))

scores.describe()

# 19.3 The apply method
# (pres
#     .select_dtypes('number')
#     .pipe(lambda df_:df_.max(axis='columns')
#     - df_.min(axis='columns'))
#     )

# apply
(pres
    .select_dtypes('number')
    .apply(lambda row: row.max()-row.min(),axis='columns')
)


# %%
