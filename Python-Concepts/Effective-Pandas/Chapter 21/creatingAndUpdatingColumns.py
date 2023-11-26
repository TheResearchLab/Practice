#%%
import pandas as pd

url = 'https://github.com/mattharrison/datasets/raw/master/data/2020-jetbrains-python-survey.csv'
df = pd.read_csv(url)
df.columns


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
(df
    [uniq_cols]
    .rename(columns=lambda c:c.replace('.','_'))
    ['age']
    .iloc[:,1:]
    ['age']
    .str.slice(0,2)
    .astype('Int64')
)
# %%
