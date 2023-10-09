from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import cumestatsteamgames, cumestatsteam, gamerotation
import pandas as pd
import numpy as np
import json
import difflib
import time
import requests

from dotenv import load_dotenv
import os
load_dotenv()
import mysql.connector as sql
import pandas as pd
from sqlalchemy import create_engine

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




# Retry Wrapper 
def retry(func, retries=3):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(30)
                attempts += 1

    return retry_wrapper

# Get Single Game aggregation columns

def getSingleGameMetrics(gameID,homeTeamID,awayTeamID,awayTeamNickname,seasonYear,gameDate):

    @retry
    def getGameStats(teamID,gameID,seasonYear):
        gameStats = cumestatsteam.CumeStatsTeam(game_ids='00'+str(gameID),league_id ="00",
                                               season=seasonYear,season_type_all_star="Regular Season",
                                               team_id = teamID).get_normalized_json()

        #print(gameStats)
        gameStats = pd.DataFrame(json.loads(gameStats)['TotalTeamStats'])

        return gameStats

    data = getGameStats(homeTeamID,gameID,seasonYear)
    data.at[1,'NICKNAME'] = awayTeamNickname
    data.at[1,'TEAM_ID'] = awayTeamID
    data.at[1,'OFFENSIVE_EFFICIENCY'] = (data.at[1,'FG'] + data.at[1,'AST'])/(data.at[1,'FGA'] - data.at[1,'OFF_REB'] + data.at[1,'AST'] + data.at[1,'TOTAL_TURNOVERS'])
    data.at[1,'SCORING_MARGIN'] = data.at[1,'PTS'] - data.at[0,'PTS']

    data.at[0,'OFFENSIVE_EFFICIENCY'] = (data.at[0,'FG'] + data.at[0,'AST'])/(data.at[0,'FGA'] - data.at[0,'OFF_REB'] + data.at[0,'AST'] + data.at[0,'TOTAL_TURNOVERS'])
    data.at[0,'SCORING_MARGIN'] = data.at[0,'PTS'] - data.at[1,'PTS']

    data['SEASON'] = seasonYear
    data['GAME_DATE'] = gameDate
    data['GAME_ID'] = gameID

    return data



sql_query = """ SELECT 
                    sss.* 
                FROM 
                    stg_nba_schedule_hist sss
                LEFT JOIN 
                    stg_game_stats sgs1 on sss.game_id = sgs1.game_id and sss.home_team_id = sgs1.team_id
                LEFT JOIN 
                    stg_game_stats sgs2 on sss.game_id = sgs2.game_id and sss.away_team_id = sgs2.team_id
                WHERE 
                    sgs1.team_id is null
                      """

# Execute the SQL query and return the result as a DataFrame
df = pd.read_sql_query(sql_query, engine)


# Load Games Based on Schedule
failed_attempts = 0
for _,row in df.iterrows():
    try:
        getSingleGameMetrics(row['GAME_ID'],row['HOME_TEAM_ID'],row['AWAY_TEAM_ID'],row['AWAY_TEAM_NICKNAME'],row['SEASON'],row['GAME_DATE']).to_sql('stg_game_stats', engine, if_exists='append', index=False, method='multi')
    except:
        print(f'failed attempt number {failed_attempts}')
        failed_attempts+= 1
        continue

conn.close()
