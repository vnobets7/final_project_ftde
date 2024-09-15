from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from avro import schema, io
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from datetime import datetime, timedelta
import time
import pickle
import os
import pandas as pd

# Load environment variables from .env file
load_dotenv()

KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
TOPIC_NAME = 'candidate'

def recruitment_selection_report(err, msg):
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
            TOPIC_NAME, 
            key=str(row["CandidateID"]),
            value=recruitment_selection_data,
            on_delivery = recruitment_selection_report
        )
        print("Produced message:", recruitment_selection_data)
    
    
# Kafka producer configuration from .env
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
    'schema.registry.basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    }

# Create a schema registry client
schema_registry_client = SchemaRegistryClient({
    "url": "pkc-97gyov.us-west3.gcp.confluent.cloud:9092",
    "basic.auth.user.info":'{}:{}'.format("schema-user", "schema-pass")
})
    
# Fetch the latest Avro schema for the value
subject_name = 'logistic_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
# Key_serializer
key_serializer = StringSerializer('utf_8')
# Create Avro Serializer for the value
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    
# Define the SerializingProducer
producer = SerializingProducer({
    "bootsrap.server": producer_conf['bootsrap.server'],
    "security.protocol": producer_conf['security.protocol'],
    "sasl.mechanisms": producer_conf['sasl.mechanisms'],
    "sasl.username": producer_conf['sasl.username'],
    "sasl.password": producer_conf['sasl.password'],
    "key.serializer": key_serializer,
    'value.serializer': avro_serializer
})
    
# Load the CSV data into a pandas DataFrame
df = pd.read_csv('recruitment_selection_data')
object_columns = df.select_dtypes(include=['object']).columns
df[object_columns] = df[object_columns].fillna('unknown value')
    
fetch_and_produce_data(producer, df)
    
# Flush the procedur
producer.flush()