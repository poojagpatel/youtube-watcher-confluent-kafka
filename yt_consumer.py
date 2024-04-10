from socket import timeout
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import sys
import logging
from pprint import pformat
from config import config


kafka_config = config['kafka_consumer_config'] | {'group.id': 'foo'}
topic_name = 'test_topic_1'
running = True
db_config = config['db_config']

def create_consumer():
    consumer = Consumer(
        kafka_config
    )
    return consumer


def insert_video_stats(conn, cur, video_data):
    cur.execute("""INSERT INTO youtube_stats (video_id, title, views, likes, comments, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)""", 
                    (video_data['video_id'], video_data['title'], video_data['views'], video_data['likes'], 
                     video_data['comments'], video_data['timestamp']))
    conn.commit()


def consume_loop(consumer):
    conn = psycopg2.connect(host= db_config['host'], dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'])
    cur = conn.cursor()

    try:
        consumer.subscribe([topic_name])
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
                data = {
                    'video_id': msg.key().decode('utf-8')
                }
                data = data | json.loads(msg.value().decode('utf-8'))
                logging.info('Got %s', pformat(data))

                try:
                    insert_video_stats(conn, cur, data)
                    logging.info('Inserted record successfully')
                except Exception as e:
                    print("Error: {}".format(e))
                    conn.rollback()
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def shutdown():
    running = False

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    consumer = create_consumer()
    consume_loop(consumer)
