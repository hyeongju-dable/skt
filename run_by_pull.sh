#!/bin/bash -
work_home=$(cd $(dirname ${0}); pwd)

docker pull chj8081/skt:v1
docker run -p 9999:9999 -t --name skt --net=host chj8081/skt:v1
sleep 5
docker container rm skt
