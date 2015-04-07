#!/usr/bin/env python

# SETUP INSTRUCTIONS:

# 1. run setup_cluster.sh once ebfore each functional testing session. See dependencies there for first run
# 2. run this from the root dir of the project - it will rebuild each time before executing to keep single command when debugging failures

from yaml import load
import os
import re
from subprocess import call
import sys

this_dir = os.path.dirname(os.path.realpath(__file__))

f = open(this_dir+"/docker-compose.yml", 'r')

cluster_config = load(f)

env_vars = ["LOG_LEVEL=DEBUG"]

broker_num = 0;

for node in cluster_config:
	match = re.match('^kafka(\d+)', node)
	if match:
		node_idx = match.group(1)
		env_vars.append('SYNKAFKA_FUNC_KAFKA_'+node_idx+'_HOST='+cluster_config[node]['environment']['KAFKA_ADVERTISED_HOST_NAME'])
		env_vars.append('SYNKAFKA_FUNC_KAFKA_'+node_idx+'_PORT='+cluster_config[node]['ports'][0].split(':')[0])
		broker_num += 1

env_vars.append('SYNKAFKA_FUNC_KAFKA_BROKER_NUM='+str(broker_num))

# recompile debug buid if needed to make it single call for debugging cycle in functional tests
rc = call('tup ./build-debug', shell=True);
if rc:
	exit(rc)

call(' '.join(env_vars) + ' ./build-debug/tests/functional/synkafka_func_test ' + ' '.join(sys.argv[1:]), shell=True);