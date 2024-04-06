import logging
from pprint import pformat
import sys
from config import config
import requests
import json
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer


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


def on_delivery(err, record):
    pass


def main():
    logging.info('START')

    schema_registry_client = SchemaRegistryClient(config['schema_registry'])
    youtube_videos_value_schema = schema_registry_client.get_latest_version('youtube_videos-value')

    kafka_config = config['kafka'] | {
        'key.serializer': StringSerializer(),
        'value.serializer': AvroSerializer(
            schema_registry_client, 
            youtube_videos_value_schema.schema.schema_str
        )
    }
    producer = SerializingProducer(kafka_config)

    google_api_key = config['google_api_key']
    youtube_playlist_id = config['youtube_playlist_id']

    

    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item['contentDetails']['videoId']

        for video in fetch_videos(google_api_key, video_id):
            logging.info('Got %s', pformat(summarize_video(video)))

            producer.produce(
                topic = "youtube_videos",
                key = video_id,
                value = {
                        'TITLE': video['snippet']['title'],
                        'VIEWS': int(video['statistics'].get('viewCount',0)),
                        'LIKES': int(video['statistics'].get('likeCount',0)),
                        'COMMENTS': int(video['statistics'].get('commentCount',0))
                    },
                on_delivery = on_delivery
            )
    
    producer.flush()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
