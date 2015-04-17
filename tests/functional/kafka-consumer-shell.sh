#!/bin/bash

export COMPOSE_PROJECT_NAME=synkafka
docker exec `docker-compose ps -q kafka2` /bin/bash -c "\$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper \$ZK_PORT_2181_TCP_ADDR $*"