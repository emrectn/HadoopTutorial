#!/bin/bash

N=${1:-3}
i=1
while [ $i -lt $N ] 
do
	docker start hadoop-slave$i
	i=$(( $i + 1))
done
docker start hadoop-master
docker ps
docker exec -it  hadoop-master bash
