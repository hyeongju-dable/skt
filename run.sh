#!/bin/bash -

# 1) prepare postgres, zookeeper and kafka
bash setting_postgres.sh
bash setting_kafka.sh
sleep 2m

# 2) task01: Inserts driver and passenger data to postres
python ./task01/create_tables.py
python ./task01/insert_data.py -s driver_booking_system -t "driver" --columns id_driver date_created name --source './resources/driver.csv'
python ./task01/insert_data.py -s driver_booking_system -t "passenger" --columns id_passenger date_created name --source './resources/passenger.csv'

# 3) task02: Executes spark streaming to summarize booking data
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 task02/weekly_summarize_booking.py &> task02.out &
sleep 2m
# 4) task01: Inserts booking data to kafka for task02
python ./task01/produce_booking.py --feature booking_id date_created id_driver id_passenger rating start_date end_date tour_value &> task01_booking.out &

# 5) task03: Calculates best relationships
spark-submit task03/cal_best_relationship.py &> task03.out

# 6) task04: Calculates KPI
spark-submit task04/create_kpi.py &> task04.out

# 7) task05: Publishes calculated data to another kafka topic
python task05/publish_kpi.py 1> task05.out

# 8) task05: Prints results of task05
cat task05.out