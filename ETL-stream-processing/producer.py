import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Kafka configuration read from .env
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("SASL_USERNAME")
KAFKA_API_SECRET = os.getenv("SASL_PASSWORD")
# SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET
}

TOPIC_NAME = 'candidate'

# Define delivery callback for confirmation
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

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
            on_delivery = delivery_report
        )
        print("Produced message:", recruitment_selection_data)

# SCHEMA REGISTRY configuration
serialization_conf = SchemaRegistryClient({
    "url": "https://psrc-405232j.us-west3.gcp.confluent.cloud",
    "basic.auth.user.info": f'{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}'
})

# Fetch the latest Avro schema for the value
subject_name = 'logistic_data-value'
schema_str = serialization_conf.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(serialization_conf,schema_str)

# Consumer configuration
producer_conf = SerializingProducer({
    'bootstrap.servers': kafka_conf['bootstrap.servers'],
    'security.protocol': kafka_conf['security.protocol'],
    'sasl.mechanisms': kafka_conf['sasl.mechanisms'],
    'sasl.username': kafka_conf['sasl.username'],
    'sasl.password': kafka_conf['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

# Load CSV file
csv_file = "data_recruitment_selection_update.csv"
topic = "candidate"

# Read CSV file using pandas
data = pd.read_csv(csv_file)
object_columns = data.select_dtypes(include=['object']).columns
data[object_columns] = data[object_columns].fillna('unknown value')

# Initialize Producer
fetch_and_produce_data(producer_conf, data)

# Wait for any remaining messages in the queue to be delivered
producer_conf.flush()

print("All messages have been sent to Kafka.")