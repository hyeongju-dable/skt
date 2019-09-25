import argparse
import traceback
import logging
import json
import collections
import time

from kafka import KafkaProducer
from kafka.client import KafkaClient
import pandas as pd

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()
stats = collections.Counter()

def parse_options():
    parser = argparse.ArgumentParser()

    parser.add_argument('-t', '--topic', dest='topic',
        default='booking',
        help='kafka topic name')
    parser.add_argument('--host', dest='host',
        nargs='+',
        default=['localhost:9092'],
        help='hostnames of kafka cluster')
    parser.add_argument('--source', dest='source',
        default='./resources/booking.csv',
        help='source file path to produce')
    parser.add_argument('--feature', dest='feature',
        nargs='+',
        help='feature name of source file')
    parser.add_argument('-N', '--dry-run', dest='dry_run',
        action='store_true',
        help='test run')
    parser.add_argument('-V', '--verbose', dest='verbose',
        action='store_true',
        help='verbose run')

    return parser.parse_args()


def on_send_success(record_metadata):
    stats['success'] += 1


def on_send_error(excp):
    stats['fail'] += 1


def main():
    global options
    options = parse_options()
    logger.setLevel(logging.DEBUG if options.verbose else logging.INFO)
    logger.info(options)

    client = KafkaClient(bootstrap_servers=options.host)
    future = client.cluster.request_update()
    client.poll(future=future)
    metadata = client.cluster
    if options.topic not in metadata.topics():
        client.add_topic(options.topic)
        logger.info('# Topic %s created', options.topic)
    else:
        logger.info('# Topic %s already exists', options.topic)

    data = pd.read_csv(options.source, names=options.feature).to_dict('records')
    producer = KafkaProducer(bootstrap_servers=options.host, value_serializer=lambda m: json.dumps(m).encode('ascii'))
    
    for row in data:
        try:
            producer.send(options.topic, row).add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            time.sleep(0.001)
        except Exception as e:
            traceback.print_exc()
            logger.debug(e)
    logger.info(stats)

if __name__ == '__main__':
    main()