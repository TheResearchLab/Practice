import streamlit as st 
import joblib 
import requests
import pandas as pd 
import numpy as np


st.sidebar.success('The Research Lab™')

st.session_state.model = joblib.load('NBAHomeTeamWinLoss.pkl')
 


url = "https://nba-prediction-api.onrender.com/all"
response = requests.get(url)
historic_games = pd.read_json(response.json(),orient='records')

# Get most recent features for away @ team
historic_games['GAME_DATE'] = pd.to_datetime(historic_games['GAME_DATE'])

url = "https://nba-prediction-api.onrender.com/upcoming_games"
response = requests.get(url)
upcoming_games = pd.read_json(response.json(),orient='records')

url = "https://nba-prediction-api.onrender.com/model_features"
response = requests.get(url)
model_features = pd.read_json(response.json(),orient='records')


def game_prediction(home_id,away_id):
    outcomes = ['AWAY TEAM WIN','HOME TEAM WIN']
    home_features = model_features[model_features['team_id'] == home_id].values[0][3:]
    away_features = model_features[model_features['team_id'] == away_id].values[0][3:]
    return outcomes[st.session_state.model.predict(np.array([*home_features,*away_features]).reshape(1,14))[0]]



#upcoming_games['pred'] = 
upcoming_games['MODEL_PREDICTION'] = upcoming_games.apply( lambda row: game_prediction(row['HOME_TEAM_ID'],row['AWAY_TEAM_ID']),axis=1)

st.title('Upcoming Games')
st.table(upcoming_games[['GAME_DATETIME','HOME_TEAM_NAME','AWAY_TEAM_NAME','MODEL_PREDICTION']])


# Get Basketball Model
# Show Accuracy and Precision
# Show game predictions table and predict