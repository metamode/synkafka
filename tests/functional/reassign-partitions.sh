#!/bin/bash

export COMPOSE_PROJECT_NAME=synkafka

# expecting $1 to be topic, $2 partition int, $3 to be new leader id $4 to be new secondary replica id
# $5 should be either --execute or --verify
if [[ $# -ne 5 ]]; then
    echo "Need 5 params: ./reassign-partitions.sh <topic : string> <partition : int> <new leader id : int> <new replica id : int> <action: --execute or --verify>"
    exit 1
fi;

docker exec `docker-compose ps -q kafka1` /bin/bash -c \
    "echo '{\"version\":1,\"partitions\":[{\"topic\":\"$1\",\"partition\":$2,\"replicas\":[$3,$4]}]}' > reassign.json && \
    \$KAFKA_HOME/bin/kafka-reassign-partitions.sh --zookeeper \$ZK_PORT_2181_TCP_ADDR --reassignment-json-file reassign.json $5"