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

import numpy as np

np.random.seed(42)

pres = tweak_siena_pres(df)
#pres.isna()
pres[~pres['Integrity'].isna()]
pres.isna().sum()


# Chapter 22.2 Duplicates
pres.drop_duplicates(subset='Party') # only return first president from each party

pres.drop_duplicates(subset='Party',keep='last') # keep defaults to first or True, can set to last or False


# only drop duplicates if previous row is duplicate

(df
    .assign(first_in_party_seq= lambda df_:df_['Party'] != df_['Party'].shift(1),
            )
    .loc[lambda df_:df_['first_in_party_seq']]
)


# Chapter 22.4 Exercises

data = {
    'CustomerID': np.arange(1, 101),
    'Age': np.random.randint(18, 65, size=100),
    'Income': np.random.normal(50000, 15000, size=100),
    'ProductPurchased': np.random.choice(['A', 'B', 'C'], size=100),
    'CustomerSatisfaction': np.random.choice([1, 2, 3, 4, 5], size=100),
    'EmailSubscription': np.random.choice([True, False], size=100),
    'MonthlySpending': np.random.uniform(50, 500, size=100),
    'ProductRating': np.random.choice([1, 2, 3, 4, 5], size=100),
    'CustomerSegment': np.random.choice(['Premium', 'Regular', 'Basic'], size=100),
    'SubscriptionDuration': np.random.choice([1, 2, 3, 4, 5], size=100)
}

income_indices = np.random.choice(np.arange(100), size=10, replace=False)
product_purchased_indices = np.random.choice(np.arange(100), size=5, replace=False)
customer_satisfaction_indices = np.random.choice(np.arange(100), size=5, replace=False)
email_subscription_indices = np.random.choice(np.arange(100), size=5, replace=False)
monthly_spending_indices = np.random.choice(np.arange(100), size=10, replace=False)
product_rating_indices = np.random.choice(np.arange(100), size=5, replace=False)
customer_segment_indices = np.random.choice(np.arange(100), size=5, replace=False)
subscription_duration_indices = np.random.choice(np.arange(100), size=10, replace=False)

# Introduce variations
data['Income'][income_indices] = np.nan
data['ProductPurchased'][product_purchased_indices] = 'D'
data['CustomerSatisfaction'][customer_satisfaction_indices] = np.random.randint(6, 10, size=5)
data['EmailSubscription'][email_subscription_indices] = 'unknown'
data['MonthlySpending'][monthly_spending_indices] = -100
data['ProductRating'][product_rating_indices] = 6
data['CustomerSegment'][customer_segment_indices] = 'PREMIUM'
data['SubscriptionDuration'][subscription_duration_indices] = -1  # Placeholder for NaN
data['SubscriptionDuration'][np.random.choice(subscription_duration_indices, size=5, replace=False)] = -3
df = pd.DataFrame(data)

df.isna().sum()
df.isna().mean() #return the pctg missing

df[df['Income'].isna()]
df.drop_duplicates()

# %%
