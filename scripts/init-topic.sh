#!/bin/sh
set -eu

BROKER="${KAFKA_BROKER:-redpanda:9092}"
TOPIC="${KAFKA_TOPIC:-otel-logs}"
PARTITIONS="${KAFKA_PARTITIONS:-2}"

rpk topic create "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replicas 1 \
  --brokers "$BROKER" ||
rpk topic describe "$TOPIC" --brokers "$BROKER"