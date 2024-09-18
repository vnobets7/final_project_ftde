#!python3

import json
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":

    with open('logFraud.json','rb') as file:
        file = json.load(file)

    producer = KafkaProducer(bootstrap_servers=['localhost'], 
                             value_serializer=json_serializer)
    
    while True:
        for data in file:
            print(data)
            producer.send("p6", data)
            time.sleep(10)
