import os
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MongoDB connection
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('MONGO_DB_NAME')
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db['myRecruitmentDatabase']

# Kafka configuration from .env
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('SECURITY_PROTOCOL'),
    'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to Kafka topic
topic = "candidate" # Change it to your current topic on confluent cloud
consumer.subscribe([topic])

def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                message_value = msg.value().decode('utf-8')
                print(f"Consumed message: {message_value}")
                # Insert the message into MongoDB
                collection.insert_one({'message': message_value})

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets
        consumer.close()

if __name__ == '__main__':
    consume_messages()
    print("Process currently done...")