from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import logging
from config import config
import requests


kafka_config = config['kafka_consumer_config'] | {'group.id': 'bar'}
topic_name = 'notifications'
running = True

def create_consumer():
    consumer = Consumer(
        kafka_config
    )
    return consumer

def send_notification(data):

    url = 'https://api.telegram.org/bot7131082839:AAGZlvSDV2ByXmMTFzgqcaqPkBRoPWqrO2I/sendMessage'

    # Form parameters
    form_data = {
        'chat_id': '6973377966',
        'text': data,
    }

    # Making the POST request
    response = requests.post(url, data=form_data)

    if response.status_code == 200:
        print("POST request successful!")
        print(response.text)
    else:
        print(f"Error: {response.status_code}")


def consume_loop(consumer):

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
                data = msg.value().decode('utf-8')
                logging.info('Got: %s', data)
                send_notification(data)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    consumer = create_consumer()
    consume_loop(consumer)

