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
away_max_dates = historic_games.groupby(['AWAY_TEAM_ID'])['GAME_DATE'].idxmax()
away_team_df = historic_games.iloc[away_max_dates].reset_index().drop(['index'],axis=1) 

# Get most recent features for team @ home
home_max_dates = historic_games.groupby(['HOME_TEAM_ID'])['GAME_DATE'].idxmax()
home_team_df = historic_games.iloc[home_max_dates].reset_index().drop(['index'],axis=1)
home_team_df 

# Get Basketball Model
# Show Accuracy and Precision
# Show game predictions table and predict