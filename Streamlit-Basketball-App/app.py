import streamlit as st 
import joblib 
import requests

st.snow()

model = joblib.load('NBAHomeTeamWinLoss.pkl')

url = "https://nba-prediction-api.onrender.com/all"
response = requests.get(url)
historic_games = pd.read_json(response.json(),orient='records')

# Get most recent features for away @ team
historic_games['GAME_DATE'] = pd.to_datetime(historic_games['GAME_DATE'])

df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
away_max_dates = historic_games.groupby(['AWAY_TEAM_ID'])['GAME_DATE'].idxmax()
away_team_df = historic_games.iloc[away_max_dates].reset_index(drop=True).rename(columns={'AWAY_TEAM_ID': 'TEAM_ID'})
away_team_df = away_team_df[['TEAM_ID','GAME_ID','GAME_DATE']]

home_max_dates = historic_games.groupby(['HOME_TEAM_ID'])['GAME_DATE'].idxmax()
home_team_df = historic_games.iloc[home_max_dates].reset_index(drop=True).rename(columns={'HOME_TEAM_ID': 'TEAM_ID'})
home_team_df = home_team_df[['TEAM_ID','GAME_ID','GAME_DATE']]

combined_df = pd.concat([home_team_df,away_team_df],ignore_index=True)
team_last_game = combined_df.iloc[combined_df.groupby(['TEAM_ID'])['GAME_DATE'].idxmax()].reset_index(drop=True)

latest_home_game = df.iloc[:,:12].merge(team_last_game, left_on=['HOME_TEAM_ID','GAME_DATE','GAME_ID'], right_on=['TEAM_ID','GAME_DATE','GAME_ID'])
latest_away_game = df.iloc[:,-11:].merge(team_last_game, left_on=['AWAY_TEAM_ID','GAME_DATE','GAME_ID'], right_on=['TEAM_ID','GAME_DATE','GAME_ID'])

st.title('Upcoming Games')

# Get Basketball Model
# Show Accuracy and Precision
# Show game predictions table and predict