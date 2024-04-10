from socket import timeout
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import sys


kafka_config = {
    'bootstrap.servers':'localhost:9092',
    'auto.offset.reset': 'earliest',
    'group.id': 'foo'
    }
running = True

def create_consumer():
    consumer = Consumer(
        kafka_config
    )
    return consumer


def consume_loop(consumer, topic):
    try:
        consumer.subscribe([topic])
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def shutdown():
    running = False

if __name__ == '__main__':
    consumer = create_consumer()
    consume_loop(consumer, 'test_topic_1')
