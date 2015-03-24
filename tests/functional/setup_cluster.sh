#!/bin/bash

# SETUP INSTRUCTIONS:

# 1. Install docker and docker-compose: https://docs.docker.com/compose/install/
# 2. Modify docker-compose to point to correct docker host IP if needed, if
#    you do need to copy it into docker-compose-local.yml which is in .gitignore but will be used if present.
# 2. Run this script at start of each testing session

command -v boot2docker >/dev/null 2>&1
if [ $? -eq 0 ]; then
	$(boot2docker shellinit)
fi

export COMPOSE_PROJECT_NAME=synkafka

if [ -e ./docker-compose-local.yml ]; then
	echo "Using docker-compose-local.yml"
	export COMPOSE_FILE=./docker-compose-local.yml ;
fi

docker-compose up -d

# Create test partitions
./kafka-topics.sh --create --topic "test" --partitions 8 --replication-factor 2