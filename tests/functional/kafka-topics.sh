#!/bin/bash

docker exec `docker-compose ps -q kafka1` /bin/bash -c "\$KAFKA_HOME/bin/kafka-topics.sh --zookeeper \$ZK_PORT_2181_TCP_ADDR $*"