# Highway Toll Streaming Data Pipeline

This project simulates toll-plaza traffic events, publishes them to
Apache Kafka, consumes the events using Python and loads the resulting records into MySQL.

## Architecture

Toll traffic generator
→ Kafka topic (`toll`)
→ Python Kafka consumer
→ MySQL table (`tolldata.livetolldata`)

## Event schema

Each Kafka event contains:

- event timestamp
- vehicle ID
- vehicle type
- toll plaza ID

## Setup

1. Install Kafka:

   ./scripts/install_kafka.sh

2. Start Kafka:

   ./scripts/initialize_kafka.sh

3. Initialize MySQL:

   ./scripts/initialize_database.sh

4. Copy and configure environment variables:

   cp .env.example .env

5. Run the streaming pipeline:

   ./scripts/run_pipeline.sh