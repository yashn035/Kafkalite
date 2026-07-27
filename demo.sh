#!/bin/bash
set -e

echo "Starting KafkaLite cluster..."
docker-compose down -v
docker-compose up -d --build

echo "Waiting for brokers to be healthy..."
until curl -sf http://localhost:8081/health >/dev/null && \
      curl -sf http://localhost:8083/health >/dev/null && \
      curl -sf http://localhost:8085/health >/dev/null; do
    echo -n "."
    sleep 1
done
echo " Cluster is healthy!"

echo "Producing initial 1000 messages..."
go run cmd/client/main.go --mode produce --broker localhost:9092 --topic test --messages 1000 --log

echo "Starting Consumer A and Consumer B..."
go run cmd/client/main.go --mode consume --broker localhost:9092 --topic test --group my-group --log --consumer-id A > consumer_a.log 2>&1 &
pid_a=$!
go run cmd/client/main.go --mode consume --broker localhost:9093 --topic test --group my-group --log --consumer-id B > consumer_b.log 2>&1 &
pid_b=$!

echo "Sleeping 5 seconds before killing leader..."
sleep 5

echo "Killing leader broker-0..."
docker stop kafkalite-broker-0

echo "Producing 500 more messages..."
go run cmd/client/main.go --mode produce --broker localhost:9093 --topic test --messages 500 --log

echo "Sleeping 5 seconds to allow failover and consumption..."
sleep 5

echo "Cleaning up..."
kill $pid_a $pid_b || true
docker-compose down -v

echo "SUCCESS"
