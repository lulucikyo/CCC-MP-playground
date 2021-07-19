#!/usr/bin/env python
import threading, time
from json import dumps, loads
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

producer = KafkaProducer(bootstrap_servers=['b-1.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092','b-2.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092','b-3.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092'],
                         value_serializer = lambda x: dumps(x).encode('utf-8'))

f = open("short.csv", "r")
f.readline()
for line in f:
    v = line.strip('\n').split(",")
    data = {'DayOfWeek':v[0],
            'FlightDate':v[1],
            'UniqueCarrier':v[2],
            'FlightNum':v[3],
            'Origin':v[4],
            'Dest':v[5],
            'CRSDepTime':v[6],
            'DepTime':v[7],
            'DepDelay':v[8],
            'CRSArrTime':v[9],
            'ArrTime':v[10],
            'ArrDelay':v[11]}
    producer.send('my-topic', value = data)
    #time.sleep(5)

f.close()
producer.close()
