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

import matplotlib.pyplot as plt
import seaborn as sns

def plot_siena_heatmap(df):
    tweaked_df = tweak_siena_pres(df)

    # Set up the figure and axis
    fig, ax = plt.subplots(figsize=(10, 10), dpi=600)

    # Create a heatmap
    g = sns.heatmap(tweaked_df
                    .set_index('President')
                    .iloc[:, 2:-1],
                    annot=True,
                    cmap='viridis',
                    ax=ax)

    # Customize x-axis labels
    g.set_xticklabels(g.get_xticklabels(), rotation=45, fontsize=8, ha='right')

    # Set the title
    _ = plt.title('Presidential Ranking')

# Assuming df is defined somewhere before this point
# Call the function to generate the heatmap
plot_siena_heatmap(df)
# %%
