import os
from confluent_kafka.avro import AvroConsumer
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration read from .env
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("SASL_USERNAME")
KAFKA_API_SECRET = os.getenv("SASL_PASSWORD")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
# MongoDB configuration read from .env
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_NAME")

TOPIC_NAME = 'candidate'

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION]

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
    'schema.registry.basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
    'group.id': 'python_example_group',
    'auto.offset.reset': 'earliest'
}

consumer = AvroConsumer(consumer_conf)
# Subscribe to the 'candidate' topic
consumer.subscribe([TOPIC_NAME])

# Function to save the consumed data into MongoDB
def save_to_mongo(data):
    collection.insert_one(data)


# Poll Kafka and save messages to MongoDB
def run_save_process():
    while True:
        try:
            msg = consumer.poll(10)
            if msg is None:
                continue

            # Print and save the message to MongoDB
            print(f"Received message: {msg.value()}")
            save_to_mongo(msg.value())
            print("Saving process done...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            consumer.close()

if __name__ == '__main__':
    run_save_process()