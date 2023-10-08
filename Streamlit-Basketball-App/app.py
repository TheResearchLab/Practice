import streamlit as st 
import joblib 
import requests

st.snow()

model = joblib.load('NBAHomeTeamWinLoss.pkl')

url = "https://nba-prediction-api.onrender.com/all"
response = requests.get(url)
historic_games = pd.read_json(response.json(),orient='records')

#upcoming_games =

# Get Basketball Model
# Show Accuracy and Precision
# Show game predictions table and predict