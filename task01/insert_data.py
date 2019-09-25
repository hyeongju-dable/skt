import argparse
import traceback
import logging
import json

import dataset
import arrow
import pandas as pd

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()

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
                        default='driver',
                        help='table name')
    parser.add_argument('--source', dest='source',
                        default='../resources/driver.csv',
                        help='source_file')
    parser.add_argument('--columns', dest='columns',
                        nargs='+',
                        help='columns names. this should be same order with source file')
    parser.add_argument('-N', '--dry-run', dest='dry_run',
                        action='store_true',
                        help='test run')
    parser.add_argument('-V', '--verbose', dest='verbose',
                        action='store_true',
                        help='verbose run')

    return parser.parse_args()


def get_db_conn():
    endpoint = '''postgresql://%(user)s:%(password)s@%(db_host)s:%(db_port)s/''' % {
        'user': options.user,
        'password': options.password,
        'db_port': options.port,
        'db_host': options.host,
    }
    logger.info(endpoint)
    conn = dataset.connect(endpoint, schema=options.scheme, engine_kwargs={
        'encoding': 'utf-8',
        'execution_options': {'isolation_level':'AUTOCOMMIT'}
    })
    return conn


def main():
    global options
    options = parse_options()
    logger.setLevel(logging.DEBUG if options.verbose else logging.INFO)
    logger.info(options)
    data = pd.read_csv(options.source, names=options.columns).to_dict('records')
    db = get_db_conn()
    db[options.table].insert_many(data)


if __name__ == '__main__':
    main()