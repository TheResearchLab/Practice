from dotenv import load_dotenv
import os
load_dotenv()
import mysql.connector as sql
import pandas as pd
from sqlalchemy import create_engine
import requests 

url = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json'
response = requests.get(url)
data = response.json()['leagueSchedule']['gameDates']

host=os.getenv("DB_HOST")
database=os.getenv("DB_NAME")
user=os.getenv("DB_USERNAME")
password=os.getenv("DB_PASSWORD")


conn = sql.connect(host=os.getenv("DB_HOST"),
                   database=os.getenv("DB_NAME"),
                   user=os.getenv("DB_USERNAME"),
                   password=os.getenv("DB_PASSWORD"),
                    )

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")


# Get keys with primitive values
def is_primitive(value):
    return isinstance(value, (int, float, str, bool))


for game_day in data:
    for scheduled_game in game_day:
        game_metadata = {key:value for key, value in game_schedule_info.items() if is_primitive(value)}
        game_metadata['homeTeamID'] = game_schedule_info['homeTeam']['teamId']
        game_metadata['awayTeamID'] = game_schedule_info['awayTeam']['teamId']
        game_metadata['homeTeamName'] = game_schedule_info['homeTeam']['teamName']
        game_metadata['awayTeamName'] = game_schedule_info['awayTeam']['teamName']

        pd.DataFrame(game_metadata,index=[0]).to_sql('stg_nba_schedule', engine, if_exists='append', index=False, method='multi')