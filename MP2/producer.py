#!/usr/bin/env python
import threading, time
from json import dumps, loads
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

producer = KafkaProducer(bootstrap_servers=['b-1.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092','b-2.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092','b-3.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092'],
                         value_serializer = lambda x: dumps(x).encode('utf-8'))

f = open("final_v2.csv", "r")
f.readline()

count = 0

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
    producer.send('test', value = data)
    count += 1
    if count % 10000==0:
        print("Sent {} records".format(count))
    #time.sleep(5)

f.close()
producer.close()
