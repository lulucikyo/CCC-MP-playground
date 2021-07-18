#!/usr/bin/env python
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

producer = KafkaProducer(bootstrap_servers=['b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094'],
                         security_protocol="SSL")

f = open("short.csv", "r")
for line in f:
    print(line)
    producer.send('my-topic', line.encode('ascii'))
    time.sleep(5)

f.close()