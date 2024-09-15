import threading
from confluent_kafka import Consumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
TOPIC_NAME = 'candidate'
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

# Kafka producer configuration from .env
consumer_conf = {
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
subject_name = 'recruitment_selection_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)


# Define the DeserializingConsumer
consumer_conf = DeserializingConsumer({
    'bootstrap.servers': consumer_conf['bootstrap.servers'],
    'security.protocol': consumer_conf['security.protocol'],
    'sasl.mechanisms': consumer_conf['sasl.mechanisms'],
    'sasl.username': consumer_conf['sasl.username'],
    'sasl.password': consumer_conf['sasl.password'],
    'key.deserializer': key_deserializer,
    'group.id': consumer_conf['group.id'],
    'auto.offset.reset': consumer_conf['auto.offset.reset']
})

# MongoDB connection
mongo_client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
db = mongo_client[MONGO_DB_NAME]
collection = db['myRecruitmentDatabase']

consumer = Consumer(consumer_conf)

# Subscribe to the 'recruitment_selection_data' topic
consumer.subscribe([TOPIC_NAME])


def consume_messages():
    # Process and insert Avro messages into MongoDB
    try:
        while True:
            msg = consumer.poll(1.0)  # Adjust the timeout as needed
            if msg is None:
                continue
            
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            value = msg.value()
            print("Received message:", value)
        
            # Data validation checks
            if value['CandidateID'] is None:
                print("Skipping message due to missing or null 'CandidateID'.")
                continue

            # Data type validation checks
            if not isinstance(value['CandidateID'], str):
                print("Skipping message due to 'CandidateID' not being a string.")
                continue
        
            # Check if a document with the same 'CandidateID' exists
            existing_document = collection.find_one({'CandidateID': value['CandidateID']})

            if existing_document:
                print(f"Document with CandidateID '{value['CandidateID']}' already exists. Skipping insertion.")
            else:
                # Insert data into MongoDB
                collection.insert_one(value)
                print("Inserted message into MongoDB:", value)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Commit the offset to mark the message as processed
        consumer.commit()
        consumer.close()
        mongo_client.close()

if __name__ == '__main__':
    consume_messages()
    print("Process currently done...")
