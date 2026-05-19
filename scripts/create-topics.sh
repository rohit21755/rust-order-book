#!/bin/bash
set -e

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
while ! nc -z localhost 29092; do
  sleep 1
done
echo "Kafka is up!"

echo "Creating Kafka topics..."

# Create topics via docker-compose exec
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic orders --partitions 12 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic trades --partitions 12 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic market-data --partitions 6 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic portfolio-events --partitions 6 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic risk-events --partitions 6 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic orderbook-updates --partitions 12 --replication-factor 1

docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic dlq.orders --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic dlq.trades --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic dlq.portfolio-events --partitions 3 --replication-factor 1

echo "Topics created successfully."
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
