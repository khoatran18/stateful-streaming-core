#!/bin/sh

# Wait for Kafka
echo "Waiting for Kafka broker to be ready..."
sleep 5

BOOTSTRAP_SERVER="broker1:29092"

echo "Creating Kafka topics..."

# 1. Init topics for Debezium Kafka Connect (cleanup.policy=compact)
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic connect-configs \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic connect-offsets \
  --partitions 5 \
  --replication-factor 1 \
  --config cleanup.policy=compact

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic connect-statuses \
  --partitions 5 \
  --replication-factor 1 \
  --config cleanup.policy=compact

# 2. Init topic for business logic
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic cdp.public.config_rules \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic input.events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic stream-schema-registry \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=100


echo "Kafka topics created successfully!"

