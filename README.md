# Real-Time Heartbeat Monitoring System

A production-grade streaming data pipeline that simulates, processes, and monitors customer heartbeat data using Kafka, PostgreSQL, and real-time visualization.

## Architecture

```
+----------------+     +----------------+     +----------------+     +------------+
| Data Generator |---->| Kafka Producer |---->|  Kafka Broker  |---->| PostgreSQL |
|   (Python)     |     | (with Schema)  |     |  + DLQ Topic   |     |  (Storage) |
+----------------+     +----------------+     +-------+--------+     +-----+------+
                                                      |                    |
                                                      v                    v
                                              +----------------+    +-------------+
                                              | Kafka Consumer |    |  Streamlit  |
                                              | + Anomaly Det. |    | (Dashboard) |
                                              | + Circuit Brkr |    +-------------+
                                              +----------------+
                                                      |
                                                      v
                                              +----------------+
                                              |  Prometheus    |---->| Grafana |
                                              |  (Metrics)     |     | (Viz)   |
                                              +----------------+     +---------+
```

## Features

- Synthetic heartbeat data generation with configurable anomaly injection
- Kafka producer with idempotent delivery and schema registry support
- Kafka consumer with batch processing and manual offset commits
- Anomaly detection (bradycardia, tachycardia, critical thresholds)
- Circuit breaker pattern for database resilience
- Dead Letter Queue for failed message handling
- Prometheus metrics with Grafana dashboard
- Structured JSON logging to files
- Streamlit real-time dashboard

## Prerequisites

- Python 3.9+
- Docker Desktop
- Git

## Quick Start

### 1. Setup Virtual Environment

```powershell
# Run setup script (creates venv, installs dependencies)
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

Wait for services to be healthy:
```bash
docker-compose ps
```

### 3. Run the Pipeline

Terminal 1 - Producer:
```bash
cd src
python kafka_producer.py
```

Terminal 2 - Consumer:
```bash
cd src
python kafka_consumer.py
```

### 4. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit | http://localhost:8501 | - |
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | - |
| Schema Registry | http://localhost:8081 | - |

## Project Structure

```
.
├── docker-compose.yml      # All services (Kafka, Postgres, Prometheus, Grafana)
├── .env                    # Configuration
├── requirements.txt        # Python dependencies
├── setup.ps1 / setup.bat   # Setup scripts
├── LEARNING_ROADMAP.md     # Learning guide
│
├── src/
│   ├── config.py           # Centralized settings
│   ├── logger.py           # File + JSON logging
│   ├── metrics.py          # Prometheus metrics
│   ├── circuit_breaker.py  # Resilience pattern
│   ├── dead_letter_queue.py # Failed message handling
│   ├── schema_registry.py  # Avro serialization
│   ├── data_generator.py   # Synthetic heartbeat data
│   ├── anomaly_detector.py # Threshold-based detection
│   ├── kafka_producer.py   # Sends to Kafka
│   ├── kafka_consumer.py   # Reads, processes, stores
│   └── health_check.py     # Service health checks
│
├── db/
│   ├── init.sql            # Schema creation
│   └── queries.sql         # Sample queries
│
├── dashboard/
│   ├── app.py              # Streamlit dashboard
│   ├── Dockerfile
│   └── requirements.txt
│
├── monitoring/
│   ├── prometheus.yml      # Prometheus config
│   └── grafana/
│       ├── provisioning/   # Auto-provisioning
│       └── dashboards/     # Pre-built dashboards
│
├── logs/                   # Log files (gitignored)
│
└── tests/
    ├── test_data_generator.py
    ├── test_anomaly_detector.py
    └── test_integration.py
```

## Configuration

Edit `.env` to modify settings:

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BOOTSTRAP_SERVERS | localhost:29092 | Kafka broker |
| MESSAGES_PER_SECOND | 10 | Producer throughput |
| CUSTOMER_COUNT | 100 | Simulated customers |
| BATCH_SIZE | 100 | Records per DB write |
| LOG_LEVEL | INFO | Logging verbosity |
| METRICS_ENABLED | true | Prometheus metrics |
| USE_SCHEMA_REGISTRY | false | Enable Avro schemas |

## Anomaly Detection

| Heart Rate | Classification | Severity |
|------------|----------------|----------|
| <= 30 BPM | critical_low | 2 |
| < 40 BPM | bradycardia | 1 |
| 40-150 BPM | normal | 0 |
| > 150 BPM | tachycardia | 1 |
| >= 180 BPM | critical_high | 2 |

## Circuit Breaker

The consumer uses a circuit breaker for database writes:

- **Closed**: Normal operation
- **Open**: After 5 consecutive failures, stops DB writes for 30 seconds
- **Half-Open**: Allows test requests to check if DB recovered

When circuit is open, messages go to the Dead Letter Queue.

## Dead Letter Queue

Failed messages are sent to `heartbeat-dlq` topic with:
- Original message content
- Error reason (deserialization, validation, database, circuit breaker)
- Timestamp and retry count

View DLQ messages:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic heartbeat-dlq --from-beginning
```

## Monitoring

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| heartbeat_messages_produced_total | Counter | Messages sent by producer |
| heartbeat_messages_consumed_total | Counter | Messages received by consumer |
| heartbeat_anomalies_detected_total | Counter | Anomalies by type and severity |
| heartbeat_db_write_duration_seconds | Histogram | Database write latency |
| heartbeat_circuit_breaker_state | Gauge | 0=closed, 1=open, 2=half-open |
| heartbeat_dlq_messages_total | Counter | Messages sent to DLQ |

### Logs

Logs are written to `logs/` directory:
- `producer.log` - Producer activity
- `consumer.log` - Consumer processing
- `*.json.log` - Structured JSON logs
- `errors.log` - All errors consolidated

View logs in real-time:
```powershell
Get-Content -Path logs\producer.log -Wait
Get-Content -Path logs\consumer.log -Wait
```

## Health Check

```bash
cd src
python health_check.py
```

## Testing

```bash
# Activate venv first
.\venv\Scripts\Activate.ps1

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html
```

## Troubleshooting

### Kafka Connection Refused

```bash
docker-compose logs kafka
# Wait 30-60 seconds after startup
```

### Database Connection Failed

```bash
docker-compose logs postgres
```

### View Consumer Lag

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group heartbeat-consumer-group
```

### Reset Consumer Offsets

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group heartbeat-consumer-group --topic heartbeat-events --reset-offsets --to-earliest --execute
```

## Stopping

```bash
docker-compose down

# Remove all data volumes
docker-compose down -v
```
