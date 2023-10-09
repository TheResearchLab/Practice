from sqlalchemy import create_engine 
from nba_api.stats.static import teams,players
import pandas as pd 
import os 
import datetime

host = os.getenv('DB_HOST')
user = os.getenv('DB_USERNAME')
database = os.getenv('DB_NAME')
password = os.getenv('DB_PASSWORD')

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

teams = pd.DataFrame(teams.get_teams())
teams['run_date'] = utc_time
teams.to_sql('stg_teams',engine,if_exists='append',index=False,method='multi')

players = pd.DataFrame(players.get_players())
players['run_date'] = utc_time
players.to_sql('stg_players',engine,if_exists='append',index=False,method='multi')

