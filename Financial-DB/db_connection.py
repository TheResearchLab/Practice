import os 
from sqlalchemy import create_engine
import mysql.connector as sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Create DB Connection
database = os.getenv('DB_NAME')
user = os.getenv('DB_USERNAME')
host = os.getenv('DB_HOST')
password = os.getenv('DB_PASSWORD')


conn = sql.connect(host=host,
                   database=database,
                   user=user,
                   password=password,
                    )

mycursor = conn.cursor()
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

