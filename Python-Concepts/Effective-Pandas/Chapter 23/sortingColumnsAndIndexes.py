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

# Chapter 23.1 Sorting Columns 
pres.sort_values(by='Party')
(pres
    .sort_values(by=['Party','Average_rank'],
                 ascending=[True,False]))

# Chapter 23.2 Sorting Column Order
pres.sort_index(axis='columns')


# Chapter 23.3 Setting and Sorting the Index
(pres
    .set_index('President')
    .sort_index()
)

# sorting allows for slicing by name when duplicated index

(pres   
    .set_index('Party')
    .sort_index()
    .loc['Democratic':'Republican']
)

# Chapter 23.5 Exercises
import random 


products = {
    101: 'Product1',
    102: 'Product2',
    103: 'Product3',
    104: 'Product4',
    105: 'Product5',
    106: 'Product6',
    107: 'Product7',
    108: 'Product8',
    109: 'Product9',
    110: 'Product10',
}

names = ['Alice', 'Bob', 'Charlie', 'David', 'Emma', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']

# Generate random data for customer orders
data = {
    'order_id': list(range(1, 101)),
    'customer_id': [random.randint(1, 10) for _ in range(100)],
    'customer_name': [random.choice(names) for _ in range(100)],
    'product_id': [random.choice(list(products.values())) for _ in range(100)],
    'quantity': [random.randint(1, 5) for _ in range(100)],
    'price_per_unit': [round(random.uniform(10.0, 50.0), 2) for _ in range(100)],
}

df = pd.DataFrame(data)
df.set_index('product_id').sort_index().loc['Prod':'Product9']

df.sort_values('price_per_unit',ascending=False)

df.sort_values(by='product_id',key=lambda x:x.str[-1])

# %%
