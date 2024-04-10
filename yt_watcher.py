from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException, SerializingProducer
from config import config
import logging
from pprint import pformat
import sys
import requests
import json



kafka_config = {
    'bootstrap.servers' : 'localhost:9092'
}


# get playlist items for a single page
def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):
    url = 'https://www.googleapis.com/youtube/v3/playlistItems'
    response = requests.get(url, params={
        'key': google_api_key,
        'playlistId': youtube_playlist_id,
        'part': 'contentDetails',
        'pageToken': page_token
    })

    payload = json.loads(response.text)
    logging.debug('GOT %s', payload)
    return payload


# get playlist items for all pages
def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):

    # fetch one page
    payload = fetch_playlist_items_page(
        google_api_key, youtube_playlist_id, page_token)

    # serve items from that page
    yield from payload['items']

    next_page_token = payload.get('nextPageToken')
    if next_page_token is not None:
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)


# get video items for a single page
def fetch_videos_page(google_api_key, video_id, page_token=None):
    url = 'https://www.googleapis.com/youtube/v3/videos'
    response = requests.get(url, params={
        'key': google_api_key,
        'id': video_id,
        'part': 'snippet,statistics',
        'pageToken': page_token
    })

    payload = json.loads(response.text)
    logging.debug('GOT %s', payload)
    return payload


# get video items for all pages
def fetch_videos(google_api_key, video_id, page_token=None):

    # fetch one page
    payload = fetch_videos_page(google_api_key, video_id, page_token)

    # serve items from that page
    yield from payload['items']

    # get next page results if exists
    next_page_token = payload.get('nextPageToken')
    if next_page_token is not None:
        yield from fetch_videos(google_api_key, video_id, next_page_token)


def summarize_video(video):
    return {
        'video_id': video['id'],
        'title': video['snippet']['title'],
        'views': int(video['statistics'].get('viewCount',0)),
        'likes': int(video['statistics'].get('likeCount',0)),
        'comments': int(video['statistics'].get('commentCount',0))
    }

def serialize_datetime(obj): 
    if isinstance(obj, datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def main():
    logging.info('START')

    producer = SerializingProducer(kafka_config)

    google_api_key = config['google_api_key']
    youtube_playlist_id = config['youtube_playlist_id']

    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item['contentDetails']['videoId']
        for video in fetch_videos(google_api_key, video_id):
            logging.info('Got %s', pformat(summarize_video(video)))

            producer.produce(
                topic = "test_topic_1",
                key = video_id,
                value = json.dumps({
                        'title': video['snippet']['title'],
                        'views': int(video['statistics'].get('viewCount',0)),
                        'likes': int(video['statistics'].get('likeCount',0)),
                        'comments': int(video['statistics'].get('commentCount',0)),
                        'timestamp': datetime.now()
                    }, default=serialize_datetime),
                on_delivery = delivery_report
            )
            
        
    
    producer.flush()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())




# {'comments': 1,
#  'likes': 8,
#  'title': 'Side Effects Are Why We Can’t Have Nice Things',
#  'video_id': 'Ime5-Yrn0o4',
#  'views': 127}