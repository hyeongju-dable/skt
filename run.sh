#!/bin/bash -
work_home=$(cd $(dirname ${0}); pwd)

docker image build  -t skt:latest .

docker run -p 9999:9999 -t --name skt --net=host skt:latest
sleep 5
docker container rm skt
