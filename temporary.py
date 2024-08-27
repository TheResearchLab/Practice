#%%


import pandas as pd
import pickle 


stringbytes = pickle.dumps('The String Hello World')

print(pd.DataFrame({'column_1':[stringbytes.hex()]}))

print(f'length of bytes: {len(stringbytes)}')
print(f'length of hex: {len(stringbytes)}')





# %%
