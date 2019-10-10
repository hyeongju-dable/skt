#!/bin/bash -

# 1) prepare postgres, zookeeper and kafka
echo "Setting postgres and kafka..."
bash setting_postgres.sh
bash setting_kafka.sh 
sleep 2m

# 2) task01: Inserts driver and passenger data to postres
echo "[task01] Inserting driver and passenger data to postres..."
python ./task01/create_tables.py
python ./task01/insert_data.py -s driver_booking_system -t "driver" --columns id_driver date_created name --source './resources/driver.csv'
python ./task01/insert_data.py -s driver_booking_system -t "passenger" --columns id_passenger date_created name --source './resources/passenger.csv'

# 3) task02: Executes spark streaming to summarize booking data
echo "[task02] Executing Spark Streaming..."
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 task02/weekly_summarize_booking.py &> task02.out &
echo "[task02] Waiting 10 minutes until the spark streaming is ready..."
sleep 10m # Waits 10 minutes to download spark-streaming-kafka
# 4) task01: Inserts booking data to kafka for task02
echo "[task01] Inserting booking data to the kafaka queue (topic: booking)..."
python ./task01/produce_booking.py --feature booking_id date_created id_driver id_passenger rating start_date end_date tour_value &> task01_booking.out &

# 5) task03: Calculates best relationships
echo "[task03] Executing the calculation of the best relationship..."
spark-submit task03/cal_best_relationship.py &> task03.out

# 6) task04: Calculates KPI
echo "[task04] Creating KPI (yearly figures for booking, driver evaluation)..."
spark-submit task04/create_kpi.py &> task04.out

# 7) task05: Publishes calculated data to another kafka topic
echo "[task05] Publish task02, task03, and task04 (topics: weekly_booking_summary, yearly_best_relationship, yearly_kpi)..."
python task05/publish_kpi.py 1> task05.out

# 8) task05: Prints results of task05
echo "[task05] Print the result of task05"
cat task05.out