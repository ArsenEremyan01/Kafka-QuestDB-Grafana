import time, json
import numpy as np
import datetime as dt

from kafka import KafkaProducer, KafkaConsumer
from config import config


def initProducer():
    # init an instance of KafkaProducer
    print('Initializing Kafka producer at {}'.format(dt.datetime.utcnow()))
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))
    return producer


def initConsumer(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    return consumer


def produceRecord(data, producer, topic, partition=0):
    # act as a producer sending records on kafka
    producer.send(topic=topic, partition=partition, value=data)


