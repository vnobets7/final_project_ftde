import os
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration read from .env
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("SASL_USERNAME")
KAFKA_API_SECRET = os.getenv("SASL_PASSWORD")
# SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
# MongoDB configuration read from .env
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_NAME")

TOPIC_NAME = 'candidate'

# Connect to MongoDB
mongo_client = MongoClient('MONGO_URI', server_api=ServerApi('1'))
db = mongo_client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION]

# SCHEMA REGISTRY configuration
serialization_conf = SchemaRegistryClient({
    "url": "https://psrc-405232j.us-west3.gcp.confluent.cloud",
    "basic.auth.user.info": f'{SCHEMA_REGISTRY_API_KEY}:{MONGO_COLLECTION}'
})

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': 'pkc-97gyov.us-west3.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'group.id': 'python_example_group',
    'auto.offset.reset': 'earliest'
}

# Fetch the latest Avro schema for the value
subject_name = 'logistic_data-value'
schema_str = serialization_conf.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(serialization_conf,schema_str)

# Consumer configuration
consumer_conf = DeserializingConsumer({
    'bootstrap.servers': kafka_conf['bootstrap.servers'],
    'security.protocol': kafka_conf['security.protocol'],
    'sasl.mechanisms': kafka_conf['sasl.mechanisms'],
    'sasl.username': kafka_conf['sasl.username'],
    'sasl.password': kafka_conf['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_conf['group.id'],
    'auto.offset.reset': kafka_conf['auto.offset.reset']
})

consumer_conf.subscribe([TOPIC_NAME])

# Function to save the consumed data into MongoDB
# def save_to_mongo(data):
#     collection.insert_one(data)
#     print("Inserted data into MongoDB success")
    

# Poll Kafka and save messages to MongoDB
def run_save_process():
    try:
        while True:
            msg = consumer_conf.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            # Deserialize Avro data
            value = msg.value()
            # Print and save the message to MongoDB
            print(f"Received message: {msg.value()}")
            # Save the consumed data into MongoDB
            collection.insert_one(value)
            print("Inserted data into MongoDB success")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer_conf.commit()
        consumer_conf.close()
        mongo_client.close()


if __name__ == '__main__':
    run_save_process()
    print("Saving process done...")