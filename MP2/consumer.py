#!/usr/bin/env python
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

consumer = KafkaConsumer(bootstrap_servers=['b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         consumer_timeout_ms=10000,
                         security_protocol="SSL",
                         group_id = "test-group")
consumer.subscribe(['my-topic'])

for message in consumer:
    print(message)

consumer.close()