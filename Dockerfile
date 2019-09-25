FROM python:3.6

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt
RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk8-jre
ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk"

# TASK 1)
CMD python ./task01/create_tables.py
CMD python ./task01/insert_data.py -s driver_booking_system -t "driver" --columns id_driver date_created name --source './resources/driver.csv'
CMD python ./task01/insert_data.py -s driver_booking_system -t "passenger" --columns id_passenger date_created name --source './resources/passenger.csv'
CMD python ./task01/produce_booking.py --feature booking_id date_created id_driver id_passenger rating start_date  end_date tour_value

# TASK 2)
CMD export SPARK_LOCAL_IP='127.0.0.1'
CMD spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 task02/weekly_summarize_booking.py &
CMD sleep 1m
CMD python ./task01/produce_booking.py --feature booking_id date_created id_driver id_passenger rating start_date  end_date tour_value

# TASK 3)
CMD spark-submit task03/cal_best_relationship.py

# TASK 4)
CMD spark-submit task04/create_kpi.py

# TASK 5)
CMD python task05/publish_kpi.py