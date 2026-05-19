#!/bin/bash

echo "Verifying Infrastructure Services..."

echo "1. Checking Zookeeper..."
echo ruok | nc localhost 2181 || echo "Zookeeper is not reachable!"

echo "2. Checking Kafka..."
nc -z localhost 29092 && echo "Kafka is reachable!" || echo "Kafka is not reachable!"

echo "3. Checking Redis..."
if command -v redis-cli &> /dev/null; then
    redis-cli ping || echo "Redis is not reachable!"
else
    echo "redis-cli not found, checking port..."
    nc -z localhost 6379 && echo "Redis port is open!" || echo "Redis port is closed!"
fi

echo "4. Checking PostgreSQL..."
nc -z localhost 5432 && echo "PostgreSQL port is open!" || echo "PostgreSQL port is closed!"

echo "5. Checking ClickHouse..."
curl -s http://localhost:8123/ping || echo "ClickHouse is not reachable!"
echo ""

echo "Verification complete."
