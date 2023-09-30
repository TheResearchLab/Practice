import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector as mysql
from nba_api.stats.endpoints import playercareerstats
import time as t

#retreive free proxy table
url = 'https://free-proxy-list.net'

response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.text,'html.parser')
    table = soup.find_all('table')[0]

    if table:
        df = pd.DataFrame()

        for row in table.find_all('tr'):
            columns = row.find_all(['td','th'])
            
            if columns:
                data = [cols.get_text(strip=True) for cols in columns]
                table_row = pd.DataFrame(pd.Series(data).values.reshape(-1,8))
                df = pd.concat([df,table_row],ignore_index=True)

df.columns = df.iloc[0]
df = df[1:].reset_index(drop=True)
#df = df[df['Https'].str.lower() == 'yes']


# Get the Proxies that work
for _,data in df.iterrows():
    try:
        t.sleep(.600)
        proxy = data['IP Address']
        playercareerstats.PlayerCareerStats(player_id='203999',proxy='http://'+proxy)
    except requests.exceptions.RequestException:
        df=df[df['IP Address']!=proxy]

df.to_csv('data.txt',sep='\t',index=False)  