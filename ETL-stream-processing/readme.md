# ETL stream processing using Python, Kafka, Confluent Cloud, Mongo Atlas, Digitalocean, and Docker end to end example

A simple example that takes .csv documents from the `candidate` topic and stores them into the `recruitment-selection-cluster` mongoDB.

The MongoDB - Kafka Source Connector also publishes all change stream events from `candidate` topic into the `recruitment-selection-cluster` mongoDB using python.

## Requirements
  - Python 3.10+
  - Docker 18.09+
  - Docker compose 1.24+
  - Dbeaver
  - VSCode
  - [Optional] MySQL workbench
  - [Optional] Pgadmin 4
  - [Optional] Confluent CLI
  - [Optional] Offset Explorer
  - [Optional] Mongo compass
  - [Optional] *nix system

## Running the example

To run the example: `./run.sh` which will:
  
  - Run `docker-compose up` 
  - Wait for PostgreSQL, MongoDB, Kafka, Zookeeper to be ready
  - Create new .env file on project folder
  - Create new environment on confluent cloud
  - Create new cluster on confluent cloud
  - Generate new API Keys for cluster
  - Create new topics on confluent cloud
  - Copy Bootstrap server + API Keys into .env
  - Create new schemas on confluent cloud
  - Generate new API Keys for schemas
  - Copy schemas registry url + API Keys into .env
  - Create new cluster on mongoDB atlas
  - Create database access 
  - Set up IP access list
  - Generate new API Keys for cluster
  - Copy mongoDB url + API Keys into .env
  - Run this command on CLI
  ```
  $ conda create -n <environment_name> python=<insert_version>
  $ conda env list
  $ conda activate <environment_name>
  $ pip install -r requirements.txt
  ```
  - Run producer.py to send data into consumer
  - Run consumer.py to recieve data and save the data into mongoDB
  - Run ML_Recruitment_Prediction.py and save  the data into postgreSQL

**Note:** The script expects to be run from within the `docs` directory and requires the whole project to be checked out / downloaded. 

## docker-compose.yml

The following systems will be created:
  - Zookeeper
  - Kafka
  - MongoDB
  - MongoDB express
  - PostgreSQL
---
