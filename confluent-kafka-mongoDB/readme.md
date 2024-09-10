# MongoDB & Kafka Docker end to end example

A simple example that takes .csv documents from the `pageviews` topic and stores them into the `test.

The MongoDB Kafka Source Connector also publishes all change stream events from `test.pageviews` into the `mongo.test.pageviews` topic.

## Requirements
  - Docker 18.09+
  - Docker compose 1.24+
  - [Optional] *nix system

## Running the example

To run the example: `./run.sh` which will:
  
  - Run `docker-compose up` 
  - Wait for MongoDB, Kafka, Kafka Connect to be ready
  - Register the Confluent Datagen Connector
  - Register the MongoDB Kafka Sink Connector
  - Register the MongoDB Kafka Source Connector
  - Publish some events to Kafka via the Datagen connector
  - Write the events to MongoDB  
  - Write the change stream messages back into Kafka

**Note:** The script expects to be run from within the `docs` directory and requires the whole project to be checked out / downloaded. 


Once running, examine the topics in the Kafka control center: http://localhost:9021/

## docker-compose.yml

The following systems will be created:

  - Zookeeper
  - Kafka
  - Confluent Schema Registry
  - Confluent Kafka Connect
  - Confluent Control Center
  - Confluent KSQL Server
  - Kafka Rest Proxy
  - Kafka Topics UI
  - MongoDB - a 3 node replicaset

---