#!/bin/bash
# Creates all Kafka topics defined in the pipeline.
# Add new topics to the TOPICS array when you add a table.

KAFKA_CONTAINER="kafka"
PARTITIONS=4
REPLICATION=1

TOPICS=(
  "companies_systemwise"
  "distributors"
  # Add new topics here
)

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  docker exec $KAFKA_CONTAINER \
    kafka-topics --create \
      --bootstrap-server localhost:9092 \
      --topic "$TOPIC" \
      --partitions $PARTITIONS \
      --replication-factor $REPLICATION \
      --config retention.ms=604800000 \
      --config max.message.bytes=10485760 \
      --if-not-exists
  echo "  Done: $TOPIC"
done

echo ""
echo "All topics:"
docker exec $KAFKA_CONTAINER \
  kafka-topics --list --bootstrap-server localhost:9092
