
import requests
import pandas as pd
from db_connection import engine

filename = r"Financial-DB\api_key.txt"

with open(filename,'r') as file:
    api_key = file.readline()

def get_exchange_df() -> pd.DataFrame:
    url = f'https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json'
    exchange_list = requests.get(url).json()
    return pd.DataFrame.from_dict(exchange_list)

def get_ticker_df(exchange_list: pd.Series) -> pd.DataFrame:
    ticker_df = pd.DataFrame()
    for exchange in exchange_list:
        url = f'https://eodhd.com/api/exchange-symbol-list/{exchange}?api_token={api_key}&fmt=json'
        ticker_list = requests.get(url).json()
        ticker_df = pd.concat([ticker_df,pd.DataFrame.from_dict(ticker_list)])
    return ticker_df.reset_index()



# main function 
chunksize = 1000
exchange_df = get_exchange_df()
ticker_df = get_ticker_df(exchange_df.Code)

exchange_df.to_sql(name="financial_db.stg_exchange",con=engine,if_exists="replace")


for i in range(0, len(ticker_df), chunksize):
    chunk = ticker_df[i:i+chunksize]  # Get the chunk of data
    chunk.to_sql("financial_db.stg_ticker", con=engine, if_exists='append', index=False)


#pd.DataFrame.from_dict(ticker_list).to_sql(name="stg_")

