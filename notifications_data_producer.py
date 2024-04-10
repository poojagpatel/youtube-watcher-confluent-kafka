from unittest import result
from confluent_kafka import SerializingProducer
import psycopg2
import logging
from config import config
import sys
import json
from yt_watcher import delivery_report, serialize_datetime
from pprint import pformat


kafka_config = config['kafka_producer_config']
topic_name = 'notifications'


def get_notification_data(cur):
    query = """
        with cte as (
            select video_id, 
                title, 
                likes likes_current, 
                timestamp, 
                lead(likes) over(partition by video_id order by timestamp desc) likes_previous
            from youtube_stats
        )

        select 
                video_id, 
                title, 
                likes_current, 
                timestamp,
                likes_previous
        from cte
        where likes_previous is not null 
            and likes_current > likes_previous;

    """
    cur.execute(query)
    records = cur.fetchall()

    # Convert fetched records into a dictionary
    result_dict = []
    column_names = [desc[0] for desc in cur.description]

    for record in records:
        result_dict.append(dict(zip(column_names, record)))
    
    return result_dict

    

def main():
    db_config = config['db_config']
    conn = psycopg2.connect(host= db_config['host'], dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'])
    cur = conn.cursor()
    results = get_notification_data(cur)
    cur.close()
    conn.close()

    producer = SerializingProducer(kafka_config)

    for data in results:
        
        logging.info('Got %s', pformat({
                            'title': data['title'],
                            'likes_current': data['likes_current'],
                            'likes_previous': data['likes_previous'],
                            'timestamp': data['timestamp'].isoformat()
                        }))
        value_str = f"Hi, likes have shot up for your video: {data['title']}! 🚀 Likes have changed from {data['likes_previous']} to {data['likes_current']}!"

        logging.info('Notification: %s', value_str)

        producer.produce(
                    topic = topic_name,
                    key = data['video_id'],
                    value = value_str,
                    on_delivery = delivery_report
                )
        
    producer.flush()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())