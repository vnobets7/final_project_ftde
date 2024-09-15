import os
import csv
import time
from confluent_kafka import Producer
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Kafka configuration from .env
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('SECURITY_PROTOCOL'),
    'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
}

# Initialize Producer
producer = Producer(conf)

# Define delivery callback for confirmation
def selection_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Load CSV file
csv_file = "data_recruitment_selection_update.csv"
topic = "candidate"

# Read CSV file using pandas
data = pd.read_csv(csv_file)

# Produce messages to Kafka
for index, row in data.iterrows():
    message = row.to_json()
    producer.produce(topic, message, callback=selection_report)
    producer.poll(0)  # Serve delivery callbacks

# Wait for any remaining messages in the queue to be delivered
producer.flush()

print("All messages have been sent to Kafka.")