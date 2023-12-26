#%%
import pandas as pd

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

# %%
