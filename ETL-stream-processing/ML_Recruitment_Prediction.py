import os
import pickle
import pandas as pd
import urllib.parse

from model import modelRecruitment
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

def insert_data_to_postgresql(df, table_name, db_url):
    try:
        engine = create_engine(db_url)

        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data telah dimasukkan ke tabel {table_name}.")
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

if __name__ == "__main__":
    # MongoDB configuration read from .env
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_NAME")

    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
    # Access a specific databasedb = client.events-logs
    db = mongo_client['recruitmentDatabase']
    # Access a collection within the database
    collection = db['myRecruitmentDatabase2']

    # Retrieve all documents from events collection
    documents = collection.find()
    # Iterate over the documents
    for document in documents:
        print(document)

    # Turn json data into DataFrame
    recruitment_df = pd.DataFrame(documents)

    # Handle missing values
    # recruitment_df.dropna()  # Drop rows with missing values
    # recruitment_df.fillna(0)  # Fill missing values with 0
    predict_result = recruitment_df

    # Get path location in variable
    pathPackages = os.getcwd()
    pathPackages = pathPackages + "\\model\\"
    os.makedirs(pathPackages, exist_ok=True)

    # Run runModel function
    result = modelRecruitment.runModel(recruitment_df, pathPackages)

    # Provide 'predict' as the column name
    predict_result['Predict'] = result

    # PostgreSQL Configuration
    username = "DWH_Ricky"
    password = "DWH_Stream"
    host = "localhost"
    port = "5433"
    database = "stream_processing_ricky"
    password = urllib.parse.quote_plus(password)

    # URL koneksi ke PostgreSQL
    db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_url)
    print("PostgreSQL DB connection success!")
    
    table_name = "Emp_Recruitment_tbl"
    # Insert data into DWH on postgreSQL
    insert_data_to_postgresql(predict_result, table_name, db_url)