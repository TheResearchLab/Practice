#%%

import requests
import pandas as pd

filename = r"Financial-DB\api_key.txt"

with open(filename,'r') as file:
    api_key = file.readline()


url = f'https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json'
exchange_list = requests.get(url).json()

url = f'https://eodhd.com/api/exchange-symbol-list/{"US"}?api_token={api_key}&fmt=json'
ticker_list = requests.get(url).json()

pd.DataFrame.from_dict(exchange_list)
pd.DataFrame.from_dict(ticker_list)
# %%
