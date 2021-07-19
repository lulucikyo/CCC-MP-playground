#!/usr/bin/env python
import threading, time
from json import dumps, loads
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

consumer = KafkaConsumer(bootstrap_servers=['b-1.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092','b-2.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092','b-3.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         consumer_timeout_ms=10000,
                         group_id = "test-group",
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
consumer.subscribe(['my-topic'])

for message in consumer:
    print(message)

consumer.close()