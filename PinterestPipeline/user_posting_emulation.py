import requests
from time import sleep
import random
import json
import argparse
import logging

from utils.database import AWSDBConnector
from utils.payloads import KafkaRestData, KinesisPackage
from utils.payloads import PinterestPayload

# random.seed(100)
logger = logging.getLogger(__name__)


def print_keys(d):
    for k in d.keys():
        print(f'`{k}`')
    print()


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
    db_connector = AWSDBConnector()
    N = num_posts

    while num_posts > 0:
        sleep(random.randrange(0, 2))
        post_data =  db_connector.random_post()
        for topic, result in zip(['pin', 'geo', 'user'], post_data):
            # pprint(result)
            if format == DataFormat.BATCH or format == DataFormat.BOTH:
                data = process_payload_kafka_json(result)
                response = send_data_kafka_processing(data, topic)
            if format == DataFormat.STREAM or format == DataFormat.BOTH:
                data = process_payload_kinesis_json(result, topic)
                response = send_data_kinesis_stream(data, topic)

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
