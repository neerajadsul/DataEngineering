from time import sleep
import random
import argparse
import logging

from utils.database import AWSDBConnector
from utils.payloads import KafkaRestData, KinesisPackage
from utils.payloads import PinterestPayload

# random.seed(100)
logger = logging.getLogger(__name__)


from enum import Enum


class DataFormat(Enum):
    """Choose between batch, streaming or both."""
    BATCH = 'kafka'
    STREAM = 'kinesis'


def run_infinite_post_data_loop(num_posts: int, format: DataFormat) -> None:
    """_summary_

    :param num_posts: _description_, defaults to -1
    """
    db_connector = AWSDBConnector()
    N = num_posts

    if format == DataFormat.BATCH:
        payload = PinterestPayload(payloader=KafkaRestData)
    if format == DataFormat.STREAM:
        payload = PinterestPayload(payloader=KinesisPackage)

    while num_posts > 0:
        sleep(random.randrange(0, 2))
        post_data =  db_connector.random_post()
        for topic, content in post_data.items():
            response = payload.send_data(content, topic)
            logger.debug(response.status_code,'\n', content)
            print(f'{topic} Sent {N - num_posts + 1} of {N}')

        num_posts -= 1
        if response.status_code == 200:
            logger.info(response.content)
        else:
            logger.error(response.content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Retrieve given number of posts')
    parser.add_argument('-n', default=1, type=int, help='number of posts, default 1')
    parser.add_argument('-f', default='kafka', type=str,
                        choices= tuple(x.value for x in DataFormat),
                        help="Data format 'kafka', 'kinesis'")
    parser.add_argument('-log', default='d', type=str, choices=('d', 'i', 'e'), help='Logging level: d, i, e')

    args = parser.parse_args()

    log_levels = {'d': logging.DEBUG, 'i': logging.INFO, 'e': logging.ERROR}

    logger.setLevel(log_levels[args.log])
    print(f'Sending {args.n} posts data to {args.f} endpoints, logging {args.log}')

    run_infinite_post_data_loop(num_posts=args.n, format=DataFormat(args.f))
