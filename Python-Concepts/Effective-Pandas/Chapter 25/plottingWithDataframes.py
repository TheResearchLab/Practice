#%%
import matplotlib.pyplot as plt
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

# Chapter 25.1 line plots

# pres.plot().legend(bbox_to_anchor=(1,1))

# fig,ax=plt.subplots(dpi=600,figsize=(10,4))
# colors = []

# def set_colors(df):
#     for col in df.columns:
#         if 'George' in col:
#             colors.append('#990000')
#         else:
#             colors.append('#999999')
#     return df
            

# (pres
#     .set_index('President')
#     .loc[::2,'Background':'Overall'] #gets every other president
#     .T
#     .pipe(set_colors)
#     .plot(ax=ax,rot=45,color=colors)
#     .legend(bbox_to_anchor=(1,1))
# )
# ax.set_xticks(range(21))
# ax.set_xticklabels(pres
#                     .loc[:,'Background':'Overall'].columns, ha='right')
# ax.set_ylabel('Rank')

# plt.show()

# 25.2 Bar Plots 

# fig,ax = plt.subplots(dpi=600, figsize=(10,4))
# (pres
#     .set_index('President')
#     .iloc[:,-5:-1]
#     .plot.bar(rot=45,figsize=(12,4),ax=ax)
# )

# ax.set_xticklabels(labels=ax.get_xticklabels(),ha='right')
# ax.legend(bbox_to_anchor=(1,1))

# (pres # easier to turn bar horizontal than rotate to read
#     .set_index('President')
#     .iloc[:,-5:-1]
#     .plot.barh(figsize=(4,12))
#     .legend(bbox_to_anchor=(1,1))
# )

# 25.3 Scatter Plots

# (pres
#     .plot.scatter(x='Integrity',y='Avoid_crucial_mistakes',
#                   c='Luck', cmap='viridis')
# )

pres.Integrity.corr(pres['Avoid_crucial_mistakes'])

(pres
    .plot.hexbin(x='Integrity',y='Avoid_crucial_mistakes',
                 cmap='Greens')
)
# %%
