#export SPARK_LOCAL_IP='127.0.0.1'
#spark-submit cal_best_relationship.py

import json
import argparse
import logging
import traceback

import arrow
import pandas as pd
import dataset
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf

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
    parser.add_argument('-s', '--scheme', dest='scheme',
                        default='driver_booking_system', help='database name')
    parser.add_argument('-t', '--table', dest='table',
                        default='yearly_best_relationship',
                        help='table name')
    parser.add_argument('--basic-time', dest='basic_time',
                        default='2016-01-01',
                        help='start date of time eg) 2016-01-01')
    parser.add_argument('--source', dest='source',
                        default='./resources/booking.csv',
                        help='file source')
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


def convert_csv2json(row, requied_cols=['id_driver', 'id_passenger']):
    cols = "booking_id date_created id_driver id_passenger rating start_date end_date tour_value".split()
    return {k: v for k, v in zip(cols, row.split(','))}


def mk_key(row):
    return':'.join([str(row['id_driver']), str(row['id_passenger'])])


def filter_feature(row):
    requied_cols = ['id_driver', 'id_passenger']
    ret = {col: row[col] for col in requied_cols}
    return ret


def flatten_data(row):
    id_driver, id_passenger = row[1].split(':')
    return {
        'id_driver': id_driver,
        'id_passenger': id_passenger,
        'relationship_score': row[0]
    }


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
    options = parse_options()

    sc = SparkContext(appName="task03")
    lines = sc.textFile(options.source)
    json_data = lines.map(lambda x: convert_csv2json(x))
    transformed_data = json_data.map(lambda x: (mk_key(x), 1))
    result = transformed_data.reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(ascending=False) \
        .map(lambda x: flatten_data(x)) \
        .take(options.topn)
    insert_data([{'basic_time': options.basic_time, 'topn': options.topn, 'json_data': json.dumps(result)}])

    
if __name__ == '__main__':
    main()
