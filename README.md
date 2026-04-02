# Webhook Notifier

A high-performance webhook delivery system built with Go, Kafka, and PostgreSQL. Handles webhook events with retry logic, dead letter queue, and fair delivery rate limiting.

## Architecture

- **Producer**: Generates webhook events and publishes to Kafka
- **Consumer**: Processes events from Kafka and delivers to webhook URLs
- **Mock API**: Test webhook receiver with configurable failure rates
- **PostgreSQL**: Stores webhook subscriptions and dead letter queue
- **Kafka**: Event streaming with 3-node cluster (KRaft mode)

## Quick Start

```bash
# Start infrastructure (Kafka + PostgreSQL + Kafka UI)
make up

# Build all components
make build

# Run the system (3 terminals)
make mockapi      # Terminal 1: webhook receiver on :8090
make consumer     # Terminal 2: webhook delivery worker
make producer     # Terminal 3: generate test events
```

Access Kafka UI at http://localhost:8080

## Components

### Producer
Generates webhook events with configurable:
- Number of messages
- Whale account simulation (high-volume accounts)
- Custom webhook URLs

### Consumer
Processes webhook events with:
- Configurable worker pool
- Retry logic with exponential backoff
- Dead Letter Queue for failed deliveries
- Fair delivery rate limiting (DRR algorithm)

### Mock API
Test webhook receiver supporting:
- Configurable failure rates
- Request logging and metrics
- Reset functionality for testing

## Configuration

Key environment variables (defaults in Makefile):
- `BROKERS`: Kafka brokers (127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092)
- `DSN`: PostgreSQL connection string
- `WEBHOOK_URL`: Target webhook URL
- `WORKERS`: Consumer worker count (default: 100)
- `MAX_RETRIES`: Maximum retry attempts (default: 3)

## Benchmarking

```bash
# Normal load: 100k messages, no whale accounts
make bench-normal

# High-volume load: 100k messages with whale accounts
make bench-whale

# Custom benchmark
NUM_MESSAGES=1000000 WORKERS=200 make bench-normal
```

## Monitoring

### Dead Letter Queue Stats
```bash
make dlq-stats
```

Shows:
- Top failing accounts by failure count
- Total failures by status
- Last failure timestamps

## Database Schema

- `webhook_subscriptions`: Active webhook endpoints per account
- `dead_letter_queue`: Failed webhook deliveries with retry metadata

## Development

```bash
make tidy     # Clean dependencies
make build    # Compile binaries
make help     # Show all commands
```

## Tech Stack

- **Go 1.26** - Core application
- **Kafka (KRaft)** - Event streaming
- **PostgreSQL** - Data persistence
- **franz-go** - Kafka client
- **Docker Compose** - Local infrastructure