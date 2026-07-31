#!/bin/sh
set -e

echo "Starting KafkaLite Broker in background..."
./broker -id 0 &

echo "Starting API Gateway in foreground..."
# PORT env var is automatically picked up by api-gateway
./api-gateway -broker localhost:9092
