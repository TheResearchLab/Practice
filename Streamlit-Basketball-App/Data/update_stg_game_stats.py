from sqlalchemy import create_engine
import pandas as pd
import os
import datetime

from nba_api.stats.endpoints import cumestatsteamgames
import time
import requests


database = os.getenv('DB_NAME')
user = os.getenv('DB_USERNAME')
host = os.getenv('DB_HOST')
password = os.getenv('DB_PASSWORD')

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

game_stats_query = "SELECT * FROM stg_game_stats"
game_stats = pd.read_sql_query(game_stats_query,engine)

nba_schedule_query = f"""SELECT  cast(gameId as signed) GAME_ID
                                ,gameDateTimeUTC as GAME_DATE_UTC
                                ,seriesText as SERIES_TEXT
                                ,homeTeamID as HOME_TEAM_ID
                                ,awayTeamID as AWAY_TEAM_ID
                                ,homeTeamName as AWAY_TEAM_NICKNAME
                                ,awayTeamName as HOME_TEAM_NICKNAME
                                ,postponedStatus as POSTPONED_STATUS
                                ,'2023-24' as SEASON
                FROM 
                    stg_nba_schedule 
                WHERE 
                    seriesText = ''
                and gameDateTimeUTC < '{datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")}'
                and postponedStatus = 'A'
                and (homeTeamId <> 0 and awayTeamID <> 0)  """

nba_schedule = pd.read_sql_query(nba_schedule_query, engine)






for _,game in nba_schedule.iterrows():
    if game['GAME_ID'] not in game_stats['GAME_ID']:
        getSingleGameMetrics(game['GAME_ID'],game['HOME_TEAM_ID'],game['AWAY_TEAM_ID'],game['AWAY_TEAM_NICKNAME'],game['SEASON'],game['GAME_DATE']).to_sql('stg_game_stats', engine, if_exists='append', index=False, method='multi')
    else:
        continue


