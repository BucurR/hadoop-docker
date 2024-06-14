#!/bin/bash

NUM_DATANODES=$1

for i in $(seq 1 $NUM_DATANODES); do
  VOLUME_NAME="datanode$(date +%s)$i"
  SERVICE_NAME="datanode_$VOLUME_NAME"
  docker volume create $VOLUME_NAME
  docker service create \
    --name $SERVICE_NAME \
    --network hadoop \
    --mount type=volume,source=$VOLUME_NAME,target=/hadoop-data/dfs/data \
    --env-file ./hadoop.env \
    --env SERVICE_PRECONDITION="namenode:9870 namenode:9000 resourcemanager:8088" \
    asergiu/ubb:hadoop-datanode
done