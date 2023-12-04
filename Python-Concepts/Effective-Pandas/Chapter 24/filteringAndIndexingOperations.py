#%%

import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/siena2018-pres.csv'
df = pd.read_csv(url, index_col=0)

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

# Chapter 24.1 Renaming an Index 
def name_to_initial(val):
    names = val.split()
    return ' '.join([f'{names[0][0]}', *names[1:]])

# (pres
#     .set_index('President')
#     .rename(name_to_initial)
# )


# Chapter 24.2 Resetting the Index
# (pres
#     .set_index('President')
#     .reset_index()
# )

# Chapter 24.3 Dataframe Indexing,Filtering, and Querying
# lt10 = pres.Average_rank < 10
# pres[lt10]

# pres[lt10 & (pres['Party'] == 'Republican')]

# pres.query('Average_rank < 10 and Party == "Republican"')


# pres.query('@lt10 and Party=="Republican"')

# # use methods in query method
# party = ['Republican']
# (pres
#     .query('Party.isin(@party)'))


# Chapter 24.4 Indexing by Position
# pres.iloc[1] # returns a series even though a row

# # return dataframe object
# type(pres.iloc[[1]])

# pres.iloc[[0,5,10]]

# pres.iloc[0:11:5] # get from index 0 to 11 (not including) and get every 5th item (follows half open)

# # passing column index 
# pres.iloc[[0,5,10],1] # return series
# pres.iloc[[0,5,10],[1]] #return dataframe


# #return columns by index 
# pres.iloc[:,[9,2,2]] # return columns at column index pres + party


# Chapter 24.5 Indexing by Name
# pres.loc['1':'5'] # behaviour seems different than book

# pres.iloc[1:5]

# (pres
#     .set_index('Party')
#     .loc[['Whig']] #returns same results even when list
# )

# Caution - this returns a list because only one entry.
# (pres
#     .set_index('Party')
#     .loc['Federalist']
# )

(pres
    .set_index('Party')
    .sort_index()
    .loc['Democratic':'Independent']
)

(pres
    .set_index('President')
    .sort_index()
    .loc['C':'Thomas Jefferson','Party':'Integrity'])

(pres
    .assign(Party=pres['Party'].astype(str)) # need to convert to string before partial indexing
    .set_index('Party')
    .sort_index()
    .loc['D':'J']    
)

(pres #partial index on the column values after sorting
    .set_index('President')
    .sort_index()
    .sort_index(axis='columns')
    .loc['C':'Thomas Jefferson','B':'D']
)
# %%
