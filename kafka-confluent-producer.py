# Databricks notebook source
from confluent_kafka import Producer, KafkaException
import logging
import time
import json
import random

def temperature_simulator():
    temperature = round(random.uniform(25, 35), 2)
    return temperature

def read_ccloud_config():
    omitted_fields = set(['schema.registry.url', 'basic.auth.credentials.source', 'basic.auth.user.info'])
    conf = {}
    client_conf_content = dbutils.fs.head("dbfs:/FileStore/client.conf")
    for line in client_conf_content.split('\n'):
        line = line.strip()
        if len(line) != 0 and line[0] != "#":
            parameter, value = line.strip().split('=', 1)
            if parameter not in omitted_fields:
                conf[parameter] = value.strip()
    return conf

def publish_to_broker():
    producer = Producer(read_ccloud_config())
    while True:
        try:
            keys = ["hall", "kitchen", "bedroom", "studyroom"]
            print("Sending data to Kafka broker")
            for i in keys:
                payload = json.dumps({"temperature": temperature_simulator()})
                producer.produce("topic_0", value=payload, key=i.encode())
                print(payload)
                time.sleep(10)
        except KafkaException as ke:
            print(ke)

if __name__ == "__main__":
    publish_to_broker()

