#export SPARK_LOCAL_IP='127.0.0.1'
#spark-submit create_kpi.py

import json
import traceback
import logging
import argparse

import dataset
import arrow
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, DateType, StringType, StructField, StructType

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()

BOOKING_QUERY_FMT = '''
SELECT
    year(date_created) AS booking_year,
    COUNT(DISTINCT id_driver) AS driver_num,
    COUNT(DISTINCT id_passenger) AS passenger_num,
    ROUND(AVG(rating), 2) AS avg_rating,
    PERCENTILE(rating, 0.75) AS percentile_25_rating,
    PERCENTILE(rating, 0.50) AS median_rating,
    PERCENTILE(rating, 0.25) AS percentile_75_rating,
    ROUND(AVG(tour_value), 2) AS avg_tour_value,
    PERCENTILE(tour_value, 0.75) AS percentile_25_tour_value,
    PERCENTILE(tour_value, 0.50) AS median_tour_value,
    PERCENTILE(tour_value, 0.25) AS percentile_75_tour_value,
    ROUND(AVG(time_delta(end_date, start_date)/60), 2) AS avg_travel_minutes
FROM booking
WHERE date_created >= '%(start_time)s'
AND date_created < '%(end_time)s'
GROUP BY booking_year
'''

DRIVER_QUERY_FMT = '''
WITH base_data AS (
    SELECT
    id_driver,
    AVG(rating) AS rating,
    AVG(tour_value) AS tour_value,
    AVG(time_delta(end_date, start_date)/60) AS travel_minutes,
    COUNT(DISTINCT id_passenger) AS passenger_cnt,
    COUNT(*) AS booking_cnt
    FROM booking
    WHERE date_created >= '%(start_time)s'
    AND date_created < '%(end_time)s'
    GROUP BY id_driver
)
SELECT ROUND(AVG(rating), 2) AS avg_rating, 
    ROUND(PERCENTILE(rating, 0.75), 2) AS percentile_25_rating,
    ROUND(PERCENTILE(rating, 0.50), 2) AS median_rating,
    ROUND(PERCENTILE(rating, 0.25), 2) AS percentile_75_rating,

    ROUND(AVG(tour_value), 2) AS avg_tour_value,
    ROUND(PERCENTILE(tour_value, 0.75), 2) AS percentile_25_tour_value,
    ROUND(PERCENTILE(tour_value, 0.50), 2) AS median_rating,
    ROUND(PERCENTILE(tour_value, 0.25), 2) AS percentile_75_tour_value,

    ROUND(AVG(travel_minutes), 2) AS avg_travel_minutes,
    ROUND(PERCENTILE(travel_minutes, 0.75), 2) AS percentile_25_travel_minutes,
    ROUND(PERCENTILE(travel_minutes, 0.50), 2) AS median_travel_minutes,
    ROUND(PERCENTILE(travel_minutes, 0.25), 2) AS percentile_75_travel_minutes,

    ROUND(AVG(passenger_cnt), 2) AS avg_passenger_cnt,
    ROUND(PERCENTILE(passenger_cnt, 0.75), 2) AS percentile_25_passenger_cnt,
    ROUND(PERCENTILE(passenger_cnt, 0.50), 2) AS median_passenger_cnt,
    ROUND(PERCENTILE(passenger_cnt, 0.25), 2) AS percentile_75_passenger_cnt,

    ROUND(AVG(booking_cnt), 2) AS avg_booking_cnt,
    ROUND(PERCENTILE(booking_cnt, 0.75), 2) AS percentile_25_booking_cnt,
    ROUND(PERCENTILE(booking_cnt, 0.50), 2) AS median_booking_cnt,
    ROUND(PERCENTILE(booking_cnt, 0.25), 2) AS percentile_75_booking_cnt,

    ROUND(AVG(booking_cnt/passenger_cnt), 2) AS avg_booking_per_passenger,
    ROUND(PERCENTILE(booking_cnt/passenger_cnt, 0.75), 2) AS percentile_25_booking_per_passenger,
    ROUND(PERCENTILE(booking_cnt/passenger_cnt, 0.50), 2) AS median_booking_per_passenger,
    ROUND(PERCENTILE(booking_cnt/passenger_cnt, 0.25), 2) AS percentile_75_booking_per_passenger
FROM base_data
'''

SCHEMA = StructType([
    StructField('booking_id', IntegerType()),
    StructField('date_created', DateType()),
    StructField('id_driver', IntegerType()),
    StructField('id_passenger', IntegerType()),
    StructField('rating', IntegerType()),
    StructField('start_date', StringType()),
    StructField('end_date', StringType()),
    StructField('tour_value', IntegerType()),
])

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
    parser.add_argument('--source', dest='source',
                        default='./resources/booking.csv', help='source file path')
    parser.add_argument('--start-time', dest='start_time',
                        default='2016-01-01 00:00:00', help='start_time')
    parser.add_argument('--end-time', dest='end_time',
                        default='2017-01-01 00:00:00', help='end_time')
    parser.add_argument('-t', '--table', dest='table',
                        default='yearly_kpi',
                        help='table name')
    parser.add_argument('-N', '--dry-run', dest='dry_run',
                        action='store_true',
                        help='test run')
    parser.add_argument('-V', '--verbose', dest='verbose',
                        action='store_true',
                        help='verbose run')

    return parser.parse_args()


def time_diff(y, x):
    a = arrow.get(y)
    b = arrow.get(x)
    print(a, b)
    return (b-a).seconds


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

    sc = SparkContext(appName="task04")
    sql_sc = SQLContext(sc)
    sql_sc.registerFunction(
        "time_delta",
        lambda y, x: (arrow.get(y) - arrow.get(x)).seconds
    )

    df = sql_sc.read.csv(options.source, SCHEMA)
    df.createOrReplaceTempView("booking")

    booking_query = BOOKING_QUERY_FMT % {
        'start_time': options.start_time,
        'end_time': options.end_time
    }
    booking_kpi = sql_sc.sql(booking_query)
    booking_kpi = booking_kpi.toPandas().to_dict('records')

    driver_query = DRIVER_QUERY_FMT % {
        'start_time': options.start_time,
        'end_time': options.end_time
    }
    driver_kpi = sql_sc.sql(driver_query)
    driver_kpi = driver_kpi.toPandas().to_dict('records')

    insert_data([{
        'basic_time': options.start_time,
        'booking_kpi': json.dumps(booking_kpi),
        'driver_kpi': json.dumps(driver_kpi)
    }])


if __name__ == '__main__':
    main()
