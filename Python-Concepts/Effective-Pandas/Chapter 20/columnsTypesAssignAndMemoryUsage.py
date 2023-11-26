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


# Chapter 20.2 Memory Usage 
#df.memory_usage(deep=True)
#pres.memory_usage(deep=True)


# same info as memory_usage method, but only prints results
#pres.info(memory_usage=True)

# Chapter 20.4 Exercises with notes
import numpy as np
import string
from random import choice

data = {}
row_cnt = 500

# for i in range(10):
#     column_name = f'column_{i}'
#     data[column_name] = np.random.uniform(1,100,50)

# base_df = pd.DataFrame(data)
# optimized_df = pd.DataFrame(data).astype({col:'float16' for col in base_df})

# display(
#     base_df.info(memory_usage=True),
#     optimized_df.info(memory_usage=True)
# )

chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
for i in range(10):
    words = ''.join(choice(chars) for _ in range(7)) 
    column_name = f'column_{i}'
    
    data[column_name] = [words for _ in range(row_cnt)] # repeated values cause savings, if all unique then strings is more efficient

base_char_df = pd.DataFrame(data).astype({col:'string' for col in data.keys()})
optimized_char_df = base_char_df.astype({col:'category' for col in data.keys()})

display(
    base_char_df.info(memory_usage=True),
    optimized_char_df.info(memory_usage=True)
)


# %%
