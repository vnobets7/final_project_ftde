import os
import csv
# import pickle
import pandas as pd
# import urllib.parse
import json
# import psycopg2

from model import modelRecruitment
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Table
# from sqlalchemy.orm import sessionmaker
from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file
from typing import Dict

# Load environment variables from .env file
load_dotenv()

def insert_data_to_postgresql(df, table_name, db_url):
    try:
        engine = create_engine(db_url)

        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"******** Data telah dimasukkan ke tabel {table_name}.******** ")
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

# Function for relationalized object
def sqlalchemy_data_type_check(dtype):
    if "int" in dtype.name:
        return Integer
    elif "object" in dtype.name:
        return String(255)
    else:
        return String(255)

# Function for relationalized object
def on_object_write(schema: str, object: dict):
    if schema not in schemas:
        schemas[schema] = Schema()
    schemas[schema].read_object(object)

def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)

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
    recruitment_collection = collection.find({})
    
    # Iterate over the documents
    # for documents in recruitment_collection:
    #     print(documents)
    print(json.dumps(recruitment_collection, default=str, indent=2))
    
    # Output CSV file
    mongoDB_output = 'mongoDB_recruitment.json'
    mongoDB_ouput_folder = 'mongoDB-data-dump-json'
    mongoDB_ouput_folder_after_convert = 'mongoDB-data-dump-csv'
    
    # Export data from mongoDB mongoDB_recruitment.json file 
    with open(mongoDB_output, "w") as export_file:
        for document in recruitment_collection:
            export_file.write(f"{json.dumps(document, default=str)}\n")
    print(f"******** Load data from mongoDB and save it into {mongoDB_output} ******** \n")
    
    # Parse the data, and split out each array into its own table
    schemas: Dict[str, Schema] = {}

    # Export data from mongoDB mongoDB_recruitment.json file and save Parsing process into 'mongoDB-data-dump' folder
    os.makedirs(mongoDB_ouput_folder, exist_ok=True)
    with Relationalize(
        MONGO_COLLECTION, create_local_file(mongoDB_ouput_folder), on_object_write
    ) as r:
        r.relationalize(create_iterator(mongoDB_output))
    print(f"******** Filter {mongoDB_output} and save it into {mongoDB_ouput_folder} ******** \n")
    
    # Type branching method to Coalescing and converting datatypes and save it into 
    os.makedirs(mongoDB_ouput_folder_after_convert, exist_ok=True)
    for schema_name, schema in schemas.items():
        with open(
            os.path.join(mongoDB_ouput_folder_after_convert, f"{schema_name}.csv"),
            "w",
        ) as final_file:
            writer = csv.DictWriter(final_file, fieldnames=schema.generate_output_columns())
            writer.writeheader()
            for row in create_iterator(os.path.join(mongoDB_ouput_folder, f"{schema_name}.json")):
                converted_obj = schema.convert_object(row)
                writer.writerow(converted_obj)
    print(f"******** Converting {mongoDB_output} to csv file and save it into {mongoDB_ouput_folder_after_convert} ******** \n")
          
    # Turn json data into DataFrame
    recruitment_df = pd.read_csv('././mongoDB-data-dump-csv/myRecruitmentDatabase2.csv')
    # print(recruitment_df.head())
    
    # Handle missing values
    recruitment_df.dropna()  # Drop rows with missing values
    recruitment_df.fillna(0)  # Fill missing values with 0
    predict_result = recruitment_df

    # # Get path location in variable
    # pathPackages = os.getcwd()
    # pathPackages = pathPackages + "\\model\\"
    # os.makedirs(pathPackages, exist_ok=True)

    # # Run runModel function
    # result = modelRecruitment.runModel(recruitment_df, pathPackages)

    # # Provide 'predict' as the column name
    # predict_result['Predict'] = result
    
    # PostgreSQL Configuration
    pg_username = "DWH_Ricky"
    pg_password = "DWH_Stream"
    pg_host = "localhost"
    pg_port = "5433"
    pg_database = "stream_processing_ricky"
    # pg_passwords = urllib.parse.quote_plus(pg_password)

    # PostgreSQL connection setup
    db_url = f"postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    engine = create_engine(db_url)
    table_name = "Emp_Recruitment_tbl"
    print("******** PostgreSQL DB connection success! ******** ")
        
    # Reflect existing tables (you can also define the table)
    metadata = MetaData()

    # 
    postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in recruitment_df.dtypes.items()]
    postgres_table = Table(table_name, metadata, 
                           *postres_columns
    )
    postgres_table.create(engine)
    print(f"******** Success create {table_name} table in PostgreSQL. ******** \n")
    
    # Insert data into DWH on postgreSQL
    insert_data_to_postgresql(recruitment_df, table_name, db_url)
