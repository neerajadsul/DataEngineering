import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
from pprint import pprint
import argparse
import yaml
import logging
from pathlib import Path
import sys


# random.seed(100)
logger = logging.getLogger(__name__)
root_dir = Path(__file__).resolve().parents[0]


def print_keys(d):
    for k in d.keys():
        print(f'`{k}`')
    print()


class AWSDBConnector:
    def __init__(self):
        with open(root_dir / 'db_creds_pinterest.yaml') as f:
            creds = yaml.safe_load(f)
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4'
        )
        return engine


def send_data_kafka_processing(data, topic):
    INVOKE_ENDPOINT = ' https://y62ln2mwxb.execute-api.us-east-1.amazonaws.com/test/topics/'
    TOPIC_ARG = f'0a1d8948160f.{topic}'

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    response = requests.request(
        method='POST',
        url=INVOKE_ENDPOINT + TOPIC_ARG,
        data=data,
        headers=headers,
    )
    response = requests.get(INVOKE_ENDPOINT)

    return response


def process_payload_kafka_json(data):

    if 'timestamp' in data:
        data['timestamp'] = data['timestamp'].isoformat()

    if 'date_joined' in data:
        data['date_joined'] = data['date_joined'].isoformat()

    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    })
    return payload


def send_data_kinesis_stream(data, topic):
    INVOKE_ENDPOINT = 'https://y62ln2mwxb.execute-api.us-east-1.amazonaws.com/test/'
    STREAM_PATH = f'streams/streaming-0a1d8948160f-{topic}/record'

    headers = {'Content-Type': 'application/json'}

    response = requests.put(
        url=INVOKE_ENDPOINT + STREAM_PATH,
        data=data,
        headers=headers,
    )

    return response


def process_payload_kinesis_json(data: dict, topic):

    if 'timestamp' in data:
        data['timestamp'] = data['timestamp'].isoformat()

    if 'date_joined' in data:
        data['date_joined'] = data['date_joined'].isoformat()

    payload = json.dumps({
        "StreamName": f"streaming-0a1d8948160f-{topic}",
        "Data": data,
        "PartitionKey": "partition-1"
    })
    return payload


from enum import Enum


class DataFormat(Enum):
    """Choose between batch, streaming or both."""
    BATCH = 'kafka'
    STREAM = 'kinesis'
    BOTH = 'both'
    


def run_infinite_post_data_loop(num_posts: int, format: DataFormat) -> None:
    """_summary_

    :param num_posts: _description_, defaults to -1
    """
    new_connector = AWSDBConnector()
    N = num_posts
    while num_posts > 0:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f'SELECT * FROM pinterest_data LIMIT {random_row}, 1')
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f'SELECT * FROM geolocation_data LIMIT {random_row}, 1')
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f'SELECT * FROM user_data LIMIT {random_row}, 1')
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            for topic, result in zip(['pin', 'geo', 'user'], [pin_result, geo_result, user_result]):
                # pprint(result)
                if format == DataFormat.BATCH or format == DataFormat.BOTH:
                    data = process_payload_kafka_json(result)
                    response = send_data_kafka_processing(data, topic)
                elif format == DataFormat.STREAM or format == DataFormat.BOTH:
                    data = process_payload_kinesis_json(result, topic)
                    response = send_data_kinesis_stream(data, topic)
                else:
                    raise ValueError('Invalid format specified, check --help.')

                logger.debug(response.status_code,'\n', data)
                print(f'{topic} Sent {N - num_posts + 1} of {N}')

            num_posts -= 1
            if response.status_code == 200:
                logger.info(response.content)
            else:
                logger.error(response.content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Retrieve given number of posts')
    parser.add_argument('-n', default=1, type=int, help='number of posts, default 1')
    parser.add_argument('-f', default=DataFormat.BOTH, type=str,
                        choices= tuple(x.value for x in DataFormat),
                        help="Data format 'kafka', 'kinesis' or 'both'")
    parser.add_argument('-log', default='d', type=str, choices=('d', 'i', 'e'), help='Logging level: d, i, e')

    args = parser.parse_args()

    log_levels = {'d': logging.DEBUG, 'i': logging.INFO, 'e': logging.ERROR}

    logger.setLevel(log_levels[args.log])
    print(f'Sending {args.n} posts data to {args.f} endpoints, logging {args.log}')

    run_infinite_post_data_loop(num_posts=args.n, format=DataFormat(args.f))
