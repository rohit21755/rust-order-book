#!/bin/bash
set -e

echo "Starting HFT Platform Infrastructure..."
docker-compose up -d

echo "Waiting for all services to become healthy..."
# A simple loop to check health status of docker-compose services
MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    UNHEALTHY=$(docker-compose ps | grep -i "unhealthy" | wc -l | tr -d ' ')
    STARTING=$(docker-compose ps | grep -i "starting" | wc -l | tr -d ' ')
    
    if [ "$UNHEALTHY" -eq "0" ] && [ "$STARTING" -eq "0" ]; then
        echo "All services are healthy!"
        break
    fi
    
    echo "Waiting for services... (Unhealthy: $UNHEALTHY, Starting: $STARTING)"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "Error: Services did not become healthy in time."
    docker-compose ps
    exit 1
fi

echo "Creating Kafka topics..."
./scripts/create-topics.sh

echo "Infrastructure is up and running!"
