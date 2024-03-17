import os 
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get DB Credentials
database = os.getenv('DB_NAME')
user = os.getenv('DB_USERNAME')
host = os.getenv('DB_HOST')
password = os.getenv('DB_PASSWORD')

# Create Session
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
Session = sessionmaker(bind=engine)
session = Session() 
