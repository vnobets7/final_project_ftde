import threading
from time import sleep
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from avro import schema, io
from kafka import KafkaProducer
from kafka.errors import KafkaError

from datetime import datetime, timedelta
import time
import pickle
import pandas as pd

def recruitment_selection_test(err, msg):
    if err is not None:
        print("Delivery failed for user record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def fetch_and_produce_data(producer, data):
    for index, row in data.iterrows():
        # include all fields from the CSV file in the data_recruitment_selection dictionary
        recruitment_selection_data = {
            "CandidateID": row["CandidateID"],
            "Name": row["Name"],
            "Gender": row["Gender"],
            "Age": row["Age"],
            "Position": row["Position"],
            "ApplicationDate": row["ApplicationDate"],
            "Status": row["Status"],
            "InterviewDate": row["InterviewDate"],
            "OfferStatus": row["OfferStatus"]
        }
        
        # Produce to kafka with CandidateID as key
        producer.produce(
            topic='recruitment_selection_data', 
            key=str(row["CandidateID"]),
            value=recruitment_selection_data,
            on_delivery = recruitment_selection_test
        )
        print("Produced message:", logistic_data)
    
    # Define kafka configuration
    kafka_config = { # Check in your confluent web
        "bootsrap.server": "",
        "security.protocol": "",
        "sasl.mechanisms": "",
        "sasl.username": "",
        "sasl.password": ""
    }
    
    # Create a schema registry client
    schema_registry_client = SchemaRegistryClient({
        "url": "",
        "basic.auth.user.info":'{}:{}'.format("schema-user", "schema-pass")
    })
    
    # Key_serializer
    key_serializer = StringSerializer('utf_8')
    
    # Define the SerializingProducer
    producer = SerializingProducer({
        "bootsrap.server": kafka_config['bootsrap.server'],
        "security.protocol": kafka_config['security.protocol'],
        "sasl.mechanisms": kafka_config['sasl.mechanisms'],
        "sasl.username": kafka_config['sasl.username'],
        "sasl.password": kafka_config['sasl.password'],
        "key.serializer": key_serializer
    })
    
    # Load the CSV data into a pandas DataFrame
    df = pd.read_csv('recruitment_selection_data')
    object_columns = df.select_dtypes(include=['object']).columns
    df[object_columns] = df[object_columns].fillna('unknown value')
    
    fetch_and_produce_data(producer, df)
    
    # Flush the procedur
    producer.flush()
