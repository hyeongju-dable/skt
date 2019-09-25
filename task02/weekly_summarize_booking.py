#export SPARK_LOCAL_IP='127.0.0.1'
#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 weekly_summarze_booking.py

import json
import argparse
import logging
import traceback

import arrow
import dataset
import pandas as pd
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()
db = None

def parse_options():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-u', '--user',
                        dest='user', default='postgres', help='user name of db')
    parser.add_argument('-p', '--password',
                        default='postpassword',
                        dest='password', help='password of db')
    parser.add_argument('-H', '--host',
                        dest='host', default='127.0.0.1', help='hostname of postgres')
    parser.add_argument('--port', dest='port',
                        default=5432, help='hostname of postgres')
    parser.add_argument('--broker', dest='broker',
                        default='localhost:9092', help='kafka broker endpoint')
    parser.add_argument('--topic', dest='topic',
                        default='booking', help='kafka topic to insert')
    parser.add_argument('-s', '--scheme', dest='scheme',
                        default='driver_booking_system', help='database name')
    parser.add_argument('-t', '--table', dest='table',
                        default='weekly_booking_summary',
                        help='table name')
    parser.add_argument('--topn', dest='topn',
                        default=10,
                        help='store topn driver')
    parser.add_argument('-N', '--dry-run', dest='dry_run',
                        action='store_true',
                        help='test run')
    parser.add_argument('-V', '--verbose', dest='verbose',
                        action='store_true',
                        help='verbose run')

    return parser.parse_args()


def mk_key(id_driver, datetime):
    datetime = arrow.get(datetime)
    weekly_time = datetime.replace(days=-datetime.weekday()).format('YYYY-MM-DD')
    return ':'.join([str(id_driver), weekly_time])


def cal_weekly_key(datetime):
    datetime = arrow.get(datetime)
    return datetime.replace(days=-datetime.weekday()).format('YYYY-MM-DD')


def cal_topn_driver(json_data, topn=10):
    json_data = [row for row in  json_data]
    cols = ['id_driver', 'rating', 'tour_value']
    data = pd.DataFrame(json_data)[cols]
    data['tour_value'] = data['tour_value'].apply(lambda x: min(x / 200.0, 5))
    data = data.groupby('id_driver')['rating', 'tour_value'].mean()
    data['score'] = data['rating'] + data['tour_value']
    return [{'id_driver': k, 'score': round(v, 2)} \
        for k, v in data['score'].nlargest(topn).to_dict().items()]


def get_db_conn():
    global db
    endpoint = '''postgresql://%(user)s:%(password)s@%(db_host)s:%(db_port)s/''' % {
        'user': options.user,
        'password': options.password,
        'db_port': options.port,
        'db_host': options.host,
    }
    logger.info(endpoint)
    if db is not None:
        logger.info('# Reuse db conection')
        return db
    db = dataset.connect(endpoint, schema=options.scheme, engine_kwargs={
        'encoding': 'utf-8',
        'execution_options': {'isolation_level':'AUTOCOMMIT'}
    })
    return db


def insert_data(data):
    try:
        db = get_db_conn()
        db[options.table].insert_many(data)
    except Exception as e:
        traceback.print_exc()
        logger.debug(e)


def main():
    global options
    global db
    options = parse_options()

    sc = SparkContext(appName="task02")
    ssc = StreamingContext(sc, 120)
    
    stream = KafkaUtils.createDirectStream(ssc, [options.topic], {"metadata.broker.list": options.broker})

    json_data = stream.map(lambda x: json.loads(x[1]))
    weekly_data = json_data.map(lambda x: (cal_weekly_key(x['end_date']), x))
    weekly_grouped_data = weekly_data.groupByKey() \
        .map(lambda x: (x[0], cal_topn_driver(x[1], topn=options.topn)))
    
    results = weekly_grouped_data.map(lambda x: [{'basic_time': x[0], 'json_data': json.dumps(x[1])}]) \
        .reduce(lambda x, y: x+y) \
        .map(lambda x: insert_data(x))
    results.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
