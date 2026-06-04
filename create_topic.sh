#!/bin/bash
# Run this after docker-compose up to create the topic

KAFKA_CONTAINER="kafka"
TOPIC="companies_systemwise"
PARTITIONS=4
REPLICATION=1

echo "Creating topic: $TOPIC"

docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --config retention.ms=604800000 \
    --config max.message.bytes=10485760 \
    --if-not-exists

echo ""
echo "Topic list:"
docker exec $KAFKA_CONTAINER \
  kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Topic details:"
docker exec $KAFKA_CONTAINER \
  kafka-topics --describe --topic $TOPIC --bootstrap-server localhost:9092
