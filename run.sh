#!/bin/bash -
work_home=$(cd $(dirname ${0}); pwd)

export POSTGRES_HOSTNAME=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postpassword"
export POSTGRES_SCHEME="driver_booking_system"

export KAFKA_HOSTNAME='kafka1'
export KAFKA_PORT=9092

export DRIVER_PATH=${work_home}/resources/driver.csv
export PASSENGER_PATH=${work_home}/resources/passenger.csv

#docker-compose -f docker_compse.yml up

docker image build --build-arg POSTGRES_HOSTNAME=$POSTGRES_HOSTNAME --build-arg POSTGRES_PORT=$POSTGRES_PORT \
    --build-arg POSTGRES_USER=$POSTGRES_USER \
    --build-arg POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -t task01:latest .
docker run -p 8081:8081 -t --name task01 --net=host task01:latest
sleep 5
docker container rm task01
