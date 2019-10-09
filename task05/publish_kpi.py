import argparse
import traceback
import logging
import json
import time
import collections

import dataset
from kafka import KafkaProducer
from kafka.client import KafkaClient

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()
stats = collections.Counter()

QUERY_FMT = '''
SELECT *
FROM %(scheme)s.%(table)s
WHERE basic_time >= '%(start_time)s'
AND basic_time < '%(end_time)s'
'''

def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--topic', dest='topic',
                        nargs='+',
                        default=['weekly_booking_summary', 'yearly_best_relationship', 'yearly_kpi'],
                        help='kafka topic name')
    parser.add_argument('--kafka-host', dest='kafka_host',
                        nargs='+',
                        default=['localhost:9092'],
                        help='hostnames of kafka cluster')
    parser.add_argument('-u', '--user',
                        dest='user', default='postgres', help='user name of db')
    parser.add_argument('-p', '--password',
                        default='postpassword',
                        dest='password', help='password of db')
    parser.add_argument('-H', '--host',
                        dest='host', default='127.0.0.1', help='hostname of postgres')
    parser.add_argument('--port', dest='port',
                        default=5432, help='hostname of postgres')
    parser.add_argument('-s', '--scheme', dest='scheme',
                        default='driver_booking_system', help='database name')
    parser.add_argument('--start-time', dest='start_time',
                        default='2016-01-01 00:00:00', help='start_time')
    parser.add_argument('--end-time', dest='end_time',
                        default='2017-01-01 00:00:00', help='end_time')
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


def create_topic(topic, client):
    metadata = client.cluster
    if topic not in metadata.topics():
        client.add_topic(topic)
        logger.info('# Topic %s created', topic)
        return True
    logger.info('# Topic %s already exists', topic)
    return False


def get_db_conn():
    endpoint = '''postgresql://%(user)s:%(password)s@%(db_host)s:%(db_port)s/''' % {
        'user': options.user,
        'password': options.password,
        'db_port': options.port,
        'db_host': options.host,
    }
    logger.info(endpoint)
    db = dataset.connect(endpoint, schema=options.scheme, engine_kwargs={
        'encoding': 'utf-8',
        'execution_options': {'isolation_level': 'AUTOCOMMIT'}
    })
    return db


def get_data(topic, db):
    query = QUERY_FMT % {
        'scheme': options.scheme,
        'table': topic,
        'start_time': options.start_time,
        'end_time': options.end_time
    }
    logger.debug(query)
    res = db.query(query)
    ret = [{k: str(v) for k, v in row.items()} for row in res]
    logger.info('# Topic: %s, data: %s', topic, ret)
    return ret


def main():
    global options
    options = parse_options()
    logger.setLevel(logging.DEBUG if options.verbose else logging.INFO)
    logger.info(options)

    client = KafkaClient(bootstrap_servers=options.kafka_host)
    future = client.cluster.request_update()
    client.poll(future=future)
    producer = KafkaProducer(bootstrap_servers=options.kafka_host, value_serializer=lambda m: json.dumps(m).encode('ascii'))

    db = get_db_conn()
    for topic in options.topic:
        try:
            create_topic(topic, client)
            data = get_data(topic, db)
            for ele in data:
                print(topic, ele)
                producer.send(topic, ele).add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            time.sleep(0.001)
        except Exception as e:
            traceback.print_exc()
            logger.debug(e)


if __name__ == '__main__':
    main()