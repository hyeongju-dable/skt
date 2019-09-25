import argparse
import logging

import dataset

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()

CERATE_TABLE_QUERY_FMT = '''
CREATE TABLE IF NOT EXISTS %(database)s.%(table)s
'''

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
    parser.add_argument('-N', '--dry-run', dest='dry_run',
                        action='store_true', help='test run')
    parser.add_argument('-V', '--verbose', dest='verbose',
                        action='store_true', help='verbose run')
    return parser.parse_args()


def get_db_conn():
    endpoint = '''postgresql://%(user)s:%(password)s@%(db_host)s:%(db_port)s/''' % {
        'user': options.user,
        'password': options.password,
        'db_port': options.port,
        'db_host': options.host,
    }
    logger.info(endpoint)
    conn = dataset.connect(endpoint, engine_kwargs={
        'encoding': 'utf-8',
        'execution_options': {'isolation_level':'AUTOCOMMIT'}
    })
    return conn



def create_scheme(db):
    query = '''CREATE SCHEMA IF NOT EXISTS %(scheme)s''' % {
        'scheme': options.scheme
    }
    try:
        db.query(query)
        logger.info('# created scheme')
        return True
    except Exception as e:
        logger.error('# failed to created scheme')
        logger.error(e)
        return False


def crate_table(db):
    query = '''
    CREATE TABLE IF NOT EXISTS driver_booking_system.driver
    (
        id_driver bigint,
        date_created timestamp without time zone,
        name character varying(255),
        PRIMARY KEY (id_driver)
    );

    CREATE TABLE IF NOT EXISTS driver_booking_system.passenger
    (
        id_passenger bigint,
        date_created timestamp without time zone,
        name character varying(255),
        PRIMARY KEY (id_passenger)
    );

    CREATE TABLE IF NOT EXISTS driver_booking_system.weekly_booking_summary
    (
        basic_time DATE,
        json_data TEXT,
        PRIMARY KEY (basic_time)
    );

    CREATE TABLE IF NOT EXISTS driver_booking_system.yearly_best_relationship
    (
        basic_time DATE,
        topn INTEGER,
        json_data TEXT,
        PRIMARY KEY (basic_time)
    );


    CREATE TABLE IF NOT EXISTS driver_booking_system.yearly_kpi
    (
        basic_time DATE,
        booking_kpi TEXT,
        driver_kpi TEXT,
        PRIMARY KEY (basic_time)
    )
    '''
    try:
        db.query(query)
        logger.info('# created tables')
        return True
    except Exception as e:
        logger.error('# failed to created table')
        logger.error(e)
        return False


def main():
    global options
    options = parse_options()
    logger.setLevel(logging.DEBUG if options.verbose else logging.INFO)
    logger.info(options)

    logger.info('# Connecting to database')
    db = get_db_conn()

    is_created = create_scheme(db)
    if is_created:
        crate_table(db)


if __name__ == '__main__':
    main()
