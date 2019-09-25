#!/bin/bash -
work_home=$(cd $(dirname ${0}); pwd)

docker image build  -t skt:latest .

docker run -p 8081:8081 -t --name skt --net=host skt:latest
sleep 5
docker container rm skt
