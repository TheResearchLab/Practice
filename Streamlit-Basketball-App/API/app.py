from fastapi import FastAPI
from sqlalchemy import create_engine
import mysql.connector as sql
import pandas as pd 
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins = ["*"],
#     allow_credentials = True,
#     allow_methods = ["*"],
#     allow_headers = ["*"],

# )

host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
user =  os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")


@app.get("/all")
async def read_all():
    query = """ select * from fct_wl_features"""
    df = pd.read_sql_query(query,engine)
    return df.to_json(orient='records')


