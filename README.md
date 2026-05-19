# HFT Simulation Platform

A low-latency, distributed, event-driven trading platform simulation.

## Prerequisites
- Docker and Docker Compose
- `nc` (netcat), `curl`, `redis-cli` (optional for local checks)

## Architecture Overview
This monorepo contains a complete HFT platform with:
- **Spring Boot Services** (Auth, Order, Market Data, Portfolio, Risk Engine)
- **Rust Matching Engine** (High-performance core)
- **Infrastructure**: Kafka (Event Streaming), Redis (In-memory cache/snapshots), PostgreSQL (Primary relational DB), ClickHouse (Time-series data).

Please refer to `HFT_System_Context.md` and `docs/` for detailed architecture diagrams and system context.

## How to Run Infrastructure

1. Initialize and start the required infrastructure (Kafka, Postgres, Redis, Clickhouse):
   ```bash
   ./scripts/start.sh
   ```
   This script will start all Docker containers, wait for them to become healthy, and automatically create the required Kafka topics.

2. Verify all services are running:
   ```bash
   ./scripts/verify.sh
   ```

3. To stop the infrastructure:
   ```bash
   docker-compose down
   ```
   *Note: Using `docker-compose down -v` will wipe all persisted data.*

## Environment Variables
The system depends on `.env` file for credentials. A default `.env` is provided.

## Directory Structure
- `services/`: Java/Spring microservices
- `matching-engine/`: Rust matching engine
- `proto/`: Shared gRPC definitions
- `infra/`: Docker configurations and init scripts
- `scripts/`: Helper scripts
- `docs/`: Additional documentation
