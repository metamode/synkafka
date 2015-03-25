#!/bin/bash

export COMPOSE_PROJECT_NAME=synkafka
docker exec `docker-compose ps -q kafka1` /bin/bash -c "\$KAFKA_HOME/bin/kafka-simple-consumer-shell.sh $*"