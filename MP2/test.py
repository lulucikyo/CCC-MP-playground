#!/usr/bin/env python
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=['b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094'],
                                 security_protocol="SSL")

        while not self.stop_event.is_set():
            f = open("short.csv", "r")
            for line in f:
                producer.send('my-topic', line.encode('ascii'))
            time.sleep(1)

        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=['b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094'],
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000,
                                 security_protocol="SSL")
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    # Create 'my-topic' Kafka topic
    try:
        admin = KafkaAdminClient(bootstrap_servers=['b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094','b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094'])

        topic = NewTopic(name='my-topic',
                         num_partitions=1,
                         replication_factor=3)
        admin.create_topics([topic])
    except Exception:
        pass

    tasks = [
        Producer(),
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()