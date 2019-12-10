#!/bin/sh
docker build --build-arg cassandra_nodes_list=${1} . -t aggregationjob:latest
