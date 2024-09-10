import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer
import os
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Define Kafka configuration
kafka_config = {
    "bootstrap.servers":"pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":"kafka-user",
    "sasl.password":"kafka-pass",
    'group.id': 'group2',
    'auto.offset.reset': 'earliest'
}


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    "url": "https://psrc-kjwmg.ap-southeast-2.aws.confluent.cloud",
    "basic.auth.user.info":'{}:{}'.format("schema-user","schema-pass")})

# Fetch the latest Avro schema for the value
subject_name = 'logistic_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


# Create Avro Deserializer for the value
# key_deserializer = Avrodeserializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_deserializer = StringDeserializer('utf_8')


# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# MongoDB configuration
uri = ""
mongo_client = MongoClient(uri, server_api=ServerApi('1'))
db = mongo_client['']
collection = db['']

# Subscribe to the 'recruitment_selection_data' topic
consumer.subscribe(['recruitment_selection_data'])


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


except KeyboardInterrupt:
    pass
finally:
    # Commit the offset to mark the message as processed
    consumer.commit()
    consumer.close()
    mongo_client.close()