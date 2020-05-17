#!/usr/bin/env bash

./kafka-topics.sh --zookeeper master:2181 --topic gios --delete

./kafka-topics.sh --zookeeper master:2181 --create \
  --topic gios \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=-1
