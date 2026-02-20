# Heartbeat Monitoring System: A Complete Beginner's Walkthrough

This guide explains every part of the project in detail. By the end, you will understand how data flows from generation to visualization, and why each component exists.

---

## Table of Contents

1. [What Does This Project Do?](#what-does-this-project-do)
2. [The Big Picture](#the-big-picture)
3. [Understanding Each Component](#understanding-each-component)
   - [Data Generator](#1-data-generator)
   - [Kafka Producer](#2-kafka-producer)
   - [Apache Kafka](#3-apache-kafka-the-message-broker)
   - [Kafka Consumer](#4-kafka-consumer)
   - [PostgreSQL Database](#5-postgresql-database)
   - [Anomaly Detection](#6-anomaly-detection)
   - [Circuit Breaker](#7-circuit-breaker)
   - [Dead Letter Queue](#8-dead-letter-queue)
   - [Prometheus Metrics](#9-prometheus-metrics)
   - [Grafana Dashboard](#10-grafana-dashboard)
   - [Streamlit Dashboard](#11-streamlit-dashboard)
4. [How Data Flows Through the System](#how-data-flows-through-the-system)
5. [Configuration Explained](#configuration-explained)
6. [The Logging System](#the-logging-system)
7. [Running the Project Step by Step](#running-the-project-step-by-step)
8. [Common Questions](#common-questions)

---

## What Does This Project Do?

Imagine a hospital that monitors the heart rates of 100 patients. Each patient has a sensor that sends their heartbeat data every few seconds. This project simulates that scenario:

1. **Generates** fake (but realistic) heart rate data for 100 customers
2. **Sends** this data through a messaging system (Kafka)
3. **Processes** the data to detect dangerous heart rates (anomalies)
4. **Stores** everything in a database
5. **Visualizes** the data on dashboards

This is called a **streaming data pipeline** - data flows continuously, like water through pipes.

---

## The Big Picture

```
+-------------------+
|  Data Generator   |  <-- Creates fake heart rate readings
+--------+----------+
         |
         | JSON messages
         v
+-------------------+
|  Kafka Producer   |  <-- Sends data to Kafka
+--------+----------+
         |
         | Kafka Protocol
         v
+-------------------+
|   Apache Kafka    |  <-- Stores messages temporarily (like a queue)
|   (Message Queue) |
+--------+----------+
         |
         | Messages polled
         v
+-------------------+
|  Kafka Consumer   |  <-- Reads messages, detects anomalies
|  + Anomaly Detect |
|  + Circuit Breaker|
+--------+----------+
         |
         | SQL INSERT
         v
+-------------------+     +-------------------+
|   PostgreSQL      |---->|    Dashboards     |
|   (Database)      |     | (Grafana/Streamlit)|
+-------------------+     +-------------------+
```

**Why not just write directly to the database?**

Because in real systems:
- The data source might produce 10,000 messages per second
- The database might be slow or temporarily unavailable
- Multiple consumers might need the same data
- You need to replay old data if something fails

Kafka sits in the middle and handles all these problems.

---

## Understanding Each Component

### 1. Data Generator

**File:** `src/data_generator.py`

**What it does:** Creates fake heart rate readings that look realistic.

**How it works:**

```python
@dataclass
class HeartbeatEvent:
    customer_id: str      # e.g., "CUST-001"
    timestamp: str        # e.g., "2026-02-18T12:00:00Z"
    heart_rate: int       # e.g., 75
```

A typical generated message looks like:
```json
{
    "customer_id": "CUST-042",
    "timestamp": "2026-02-18T12:00:00.123456+00:00",
    "heart_rate": 78
}
```

**Key concepts:**

- **Normal Distribution:** Heart rates cluster around 75 BPM with natural variation. Most readings are 60-100 BPM, just like real humans.

- **Anomaly Injection:** 2% of readings are intentionally abnormal (too high or too low) to test that our detection works.

```python
def _generate_normal_heart_rate(self) -> int:
    # random.gauss creates a bell curve distribution
    # mean=80, standard deviation=15
    hr = random.gauss(self._hr_mean, self._hr_std)
    return max(45, min(145, int(hr)))  # Clamp to valid range
```

**Why this matters:** You can't test a monitoring system without data. Real patient data is private, so we generate synthetic data that behaves like real data.

---

### 2. Kafka Producer

**File:** `src/kafka_producer.py`

**What it does:** Sends generated data to Kafka.

**Key concepts:**

**a) Creating a Producer:**
```python
self._producer = Producer({
    "bootstrap.servers": "localhost:29092",  # Where is Kafka?
    "client.id": "heartbeat-producer",       # Who am I?
    "acks": "all",                           # Wait for confirmation
    "enable.idempotence": True,              # Prevent duplicates
})
```

**b) Sending a Message:**
```python
def send(self, event: dict) -> None:
    key = event["customer_id"].encode("utf-8")   # Partition key
    value = json.dumps(event).encode("utf-8")    # Message body
    
    self._producer.produce(
        topic=self.topic,           # Which topic? "heartbeat-events"
        key=key,                    # Used to determine partition
        value=value,                # The actual data
        callback=self._delivery_callback,  # Called when sent
    )
```

**c) The Partition Key:**

Kafka topics are divided into **partitions** (like lanes on a highway). The key determines which partition a message goes to.

```
Topic: heartbeat-events
├── Partition 0: CUST-001, CUST-004, CUST-007, ...
├── Partition 1: CUST-002, CUST-005, CUST-008, ...
└── Partition 2: CUST-003, CUST-006, CUST-009, ...
```

Messages with the same key always go to the same partition. This guarantees **message ordering per customer** - you'll always see CUST-001's readings in the order they were sent.

**d) Delivery Callbacks:**

When you send a message, it doesn't arrive instantly. The callback tells you what happened:

```python
def _delivery_callback(self, err, msg):
    if err:
        # Message failed to deliver
        self.stats.failed += 1
        self.logger.error(f"Delivery failed: {err}")
    else:
        # Message delivered successfully
        self.stats.delivered += 1
```

---

### 3. Apache Kafka (The Message Broker)

**Not a file - it's a service running in Docker**

**What it does:** Stores messages and delivers them to consumers.

Think of Kafka as a **very fast, very reliable post office**:
- Producers send letters (messages) to mailboxes (topics)
- The post office (Kafka) stores all letters
- Consumers pick up letters when they're ready
- Letters are kept for 7 days (configurable) even after reading

**Key concepts:**

**a) Topics:**
A topic is a named stream of messages. We use two:
- `heartbeat-events` - Normal heart rate data
- `heartbeat-dlq` - Failed messages (Dead Letter Queue)

**b) Partitions:**
Each topic is split into partitions for parallelism. More partitions = more throughput, but more complexity.

**c) Consumer Groups:**
Multiple consumers can work together in a group. Each partition is assigned to one consumer in the group.

```
Consumer Group: heartbeat-consumer-group
├── Consumer 1: Reads from Partition 0
├── Consumer 2: Reads from Partition 1  
└── Consumer 3: Reads from Partition 2
```

**d) Offsets:**
Each message has an offset (like a line number). Consumers track their position:

```
Partition 0: [msg0, msg1, msg2, msg3, msg4, msg5, ...]
                               ^
                               Consumer is here (offset 3)
```

If the consumer crashes and restarts, it continues from offset 3.

**e) Why Kafka and not just a database?**

| Feature | Database | Kafka |
|---------|----------|-------|
| Write speed | ~5,000/sec | ~100,000/sec |
| Replayability | Manual queries | Built-in offset reset |
| Multiple consumers | Single reader | Many readers, each tracks own position |
| Decoupling | Tight coupling | Producer doesn't know consumers |

---

### 4. Kafka Consumer

**File:** `src/kafka_consumer.py`

**What it does:** Reads messages from Kafka, processes them, stores in database.

**Key concepts:**

**a) Creating a Consumer:**
```python
self._consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "heartbeat-consumer-group",  # Consumer group
    "auto.offset.reset": "earliest",          # Start from beginning
    "enable.auto.commit": False,              # Manual commit
})
```

**b) The Polling Loop:**
```python
while self._running:
    msg = self._consumer.poll(timeout=1.0)  # Wait up to 1 second
    
    if msg is None:
        continue  # No message available
    
    if msg.error():
        continue  # Handle errors
    
    record = self._process_message(msg)  # Do work
    self._batch.append(record)           # Collect for batch insert
```

**c) Batch Processing:**

Writing one record at a time is slow. Instead, we collect 100 records and write them all at once:

```python
batch_size: int = 100
batch_timeout_seconds: float = 5.0
```

- If we have 100 records, write immediately
- If 5 seconds pass with fewer records, write anyway (don't wait forever)

This improves throughput by ~100x.

**d) Manual Offset Commit:**

We only commit (save our position) AFTER successfully writing to the database:

```python
written = self._db_writer.write_batch(self._batch)
self._consumer.commit()  # Only commit after successful write
```

Why? If we commit first and the database write fails, we'd skip those messages forever. This is called **at-least-once delivery** - we might process a message twice (if we crash between write and commit), but we'll never skip one.

---

### 5. PostgreSQL Database

**File:** `db/init.sql`

**What it does:** Stores all heart rate records permanently.

**The table structure:**

```sql
CREATE TABLE heartbeat_records (
    id SERIAL PRIMARY KEY,           -- Auto-incrementing ID
    customer_id VARCHAR(20) NOT NULL, -- e.g., "CUST-001"
    timestamp TIMESTAMPTZ NOT NULL,   -- When the reading was taken
    heart_rate INTEGER NOT NULL,      -- BPM value
    is_anomaly BOOLEAN DEFAULT FALSE, -- Did we detect a problem?
    anomaly_type VARCHAR(50),         -- What kind? (bradycardia, etc.)
    created_at TIMESTAMPTZ DEFAULT NOW() -- When we stored it
);
```

**Indexes (make queries fast):**

```sql
-- Find recent data quickly (most common query)
CREATE INDEX idx_heartbeat_timestamp ON heartbeat_records(timestamp DESC);

-- Find a specific customer's data
CREATE INDEX idx_heartbeat_customer ON heartbeat_records(customer_id);

-- Find anomalies only (partial index - smaller, faster)
CREATE INDEX idx_heartbeat_anomaly ON heartbeat_records(is_anomaly) 
    WHERE is_anomaly = TRUE;
```

**Why indexes matter:**

Without an index on `timestamp`, this query scans millions of rows:
```sql
SELECT * FROM heartbeat_records 
WHERE timestamp > NOW() - INTERVAL '5 minutes';
```

With an index, it jumps directly to recent data.

---

### 6. Anomaly Detection

**File:** `src/anomaly_detector.py`

**What it does:** Identifies dangerous heart rates.

**The rules:**

| Heart Rate | Classification | Severity | Medical Term |
|------------|----------------|----------|--------------|
| <= 30 BPM | Critical Low | 2 | Severe bradycardia |
| 31-39 BPM | Bradycardia | 1 | Slow heart rate |
| 40-150 BPM | Normal | 0 | Healthy range |
| 151-179 BPM | Tachycardia | 1 | Fast heart rate |
| >= 180 BPM | Critical High | 2 | Dangerous tachycardia |

**The code:**

```python
def detect(self, heart_rate: int) -> AnomalyResult:
    if heart_rate <= self.critical_low:
        return AnomalyResult(True, "critical_low", 2)
    
    if heart_rate >= self.critical_high:
        return AnomalyResult(True, "critical_high", 2)
    
    if heart_rate < self.low_threshold:
        return AnomalyResult(True, "bradycardia", 1)
    
    if heart_rate > self.high_threshold:
        return AnomalyResult(True, "tachycardia", 1)
    
    return AnomalyResult(False, None, 0)
```

**Why a separate module?**

Anomaly detection logic might change:
- Different thresholds for different customers (athletes have lower resting rates)
- Machine learning models instead of simple thresholds
- Pattern detection (sudden changes, irregular rhythms)

By isolating it, we can swap implementations without touching the consumer.

---

### 7. Circuit Breaker

**File:** `src/circuit_breaker.py`

**What it does:** Prevents cascading failures when the database is down.

**The problem it solves:**

If PostgreSQL crashes, every database write fails. Without protection:
- Consumer keeps trying to write
- Kafka keeps sending messages
- Consumer falls behind (lag increases)
- When database recovers, consumer is overwhelmed

**The solution (Circuit Breaker pattern):**

```
State: CLOSED (normal)
    │
    │ 5 consecutive failures
    v
State: OPEN (stop trying)
    │
    │ Wait 30 seconds
    v
State: HALF-OPEN (test with 1 request)
    │
    ├─ Success → CLOSED
    └─ Failure → OPEN
```

**The code:**

```python
class CircuitBreaker:
    def call(self, func, *args, **kwargs):
        if not self._should_allow_request():
            raise CircuitBreakerOpen("Circuit is open")
        
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            raise
```

**What happens when open?**

Messages go to the Dead Letter Queue instead of being lost.

---

### 8. Dead Letter Queue

**File:** `src/dead_letter_queue.py`

**What it does:** Stores messages that couldn't be processed.

**Why messages fail:**

1. **Deserialization error:** Invalid JSON
2. **Validation error:** Missing required field
3. **Database error:** PostgreSQL is down
4. **Circuit breaker open:** We stopped trying

**The DLQ message format:**

```json
{
    "original_topic": "heartbeat-events",
    "original_partition": 2,
    "original_offset": 12345,
    "original_key": "CUST-042",
    "original_value": "{\"customer_id\": \"CUST-042\", ...}",
    "error_reason": "database_error",
    "error_message": "connection refused",
    "retry_count": 0,
    "timestamp": "2026-02-18T12:00:00Z"
}
```

**What to do with DLQ messages:**

1. **Investigate:** Why did they fail?
2. **Fix the issue:** Repair database, fix code
3. **Replay:** Resend messages from DLQ to main topic

This is not automated in this project (it's an advanced topic), but the infrastructure is in place.

---

### 9. Prometheus Metrics

**File:** `src/metrics.py`

**What it does:** Exposes numbers that describe system health.

**Types of metrics:**

**a) Counter (only goes up):**
```python
messages_produced = Counter(
    "heartbeat_messages_produced_total",
    "Total messages produced",
    ["status"],  # Labels: success, failed
)

# Usage:
metrics.messages_produced.labels(status="success").inc()
```

**b) Gauge (goes up and down):**
```python
producer_queue_size = Gauge(
    "heartbeat_producer_queue_size",
    "Current producer queue size",
)

# Usage:
metrics.producer_queue_size.set(42)
```

**c) Histogram (measures distributions):**
```python
db_write_duration = Histogram(
    "heartbeat_db_write_duration_seconds",
    "Database write latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Usage:
metrics.db_write_duration.observe(0.023)  # 23ms
```

**How Prometheus collects metrics:**

The producer exposes metrics at `http://localhost:8000/metrics`:

```
# HELP heartbeat_messages_produced_total Total messages produced
# TYPE heartbeat_messages_produced_total counter
heartbeat_messages_produced_total{status="success"} 5432.0
heartbeat_messages_produced_total{status="failed"} 3.0

# HELP heartbeat_producer_queue_size Current producer queue size
# TYPE heartbeat_producer_queue_size gauge
heartbeat_producer_queue_size 12.0
```

Prometheus scrapes this endpoint every 15 seconds and stores the data.

---

### 10. Grafana Dashboard

**File:** `monitoring/grafana/dashboards/heartbeat.json`

**What it does:** Visualizes Prometheus metrics.

**The dashboard panels:**

1. **Total Messages Produced** - Counter showing throughput
2. **Total Messages Consumed** - Counter showing processing
3. **Total Anomalies** - Counter with color thresholds (turns red if high)
4. **DLQ Messages** - Counter (should be 0 normally)
5. **Throughput Graph** - Messages per second over time
6. **Anomalies by Type** - Bar chart breakdown
7. **Database Latency** - p50 and p95 percentiles
8. **Circuit Breaker Status** - Shows CLOSED/OPEN/HALF-OPEN

**How to access:**
1. Open http://localhost:3000
2. Login: admin / admin123
3. Click "Dashboards" -> "Heartbeat Monitoring"

---

### 11. Streamlit Dashboard

**File:** `dashboard/app.py`

**What it does:** Real-time dashboard built with Python.

**Why both Grafana and Streamlit?**

| Grafana | Streamlit |
|---------|-----------|
| For ops/DevOps | For business/analysts |
| Time-series focused | Custom visualizations |
| Prometheus data | Database queries |
| Industry standard | Easy to customize |

**Key features:**

1. **Summary stats** - Total records, active customers, anomaly count
2. **Scatter plot** - Heart rates over time, anomalies in red
3. **Anomaly table** - Recent dangerous readings
4. **Customer drill-down** - Select one customer, see their history

**How auto-refresh works:**

```python
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)
if auto_refresh:
    time.sleep(0.1)
    st.rerun()  # Reload the entire page
```

---

## How Data Flows Through the System

Let's trace a single heartbeat reading from generation to dashboard:

### Step 1: Generation

```python
# data_generator.py
event = HeartbeatEvent(
    customer_id="CUST-042",
    timestamp="2026-02-18T12:00:00.123456+00:00",
    heart_rate=78,
)
```

### Step 2: Serialization

```python
# kafka_producer.py
value = json.dumps(event.to_dict()).encode("utf-8")
# Result: b'{"customer_id": "CUST-042", "timestamp": "...", "heart_rate": 78}'
```

### Step 3: Send to Kafka

```python
# kafka_producer.py
self._producer.produce(
    topic="heartbeat-events",
    key=b"CUST-042",
    value=value,
)
```

### Step 4: Kafka stores the message

```
Topic: heartbeat-events
Partition 2: [..., msg12344, msg12345(our message), ...]
```

### Step 5: Consumer polls

```python
# kafka_consumer.py
msg = self._consumer.poll(timeout=1.0)
# msg.value() = b'{"customer_id": "CUST-042", ...}'
```

### Step 6: Deserialization

```python
# kafka_consumer.py
data = json.loads(msg.value().decode("utf-8"))
# data = {"customer_id": "CUST-042", "timestamp": "...", "heart_rate": 78}
```

### Step 7: Anomaly detection

```python
# kafka_consumer.py
result = self._detector.detect(78)
# result = AnomalyResult(is_anomaly=False, anomaly_type=None, severity=0)
```

### Step 8: Add to batch

```python
# kafka_consumer.py
record = {
    "customer_id": "CUST-042",
    "timestamp": "2026-02-18T12:00:00.123456+00:00",
    "heart_rate": 78,
    "is_anomaly": False,
    "anomaly_type": None,
}
self._batch.append(record)
```

### Step 9: Batch full, write to database

```python
# kafka_consumer.py (after 100 records)
execute_values(
    cur,
    "INSERT INTO heartbeat_records (...) VALUES %s",
    [tuple(r.values()) for r in self._batch],
)
```

### Step 10: Commit offset

```python
# kafka_consumer.py
self._consumer.commit()  # "I've processed up to this point"
```

### Step 11: Dashboard queries

```sql
-- dashboard/app.py
SELECT * FROM heartbeat_records 
WHERE timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp DESC;
```

### Step 12: User sees the data

The scatter plot shows a blue dot at (12:00:00, 78 BPM).

---

## Configuration Explained

**File:** `.env`

Every setting is externalized so you can change behavior without editing code:

```ini
# Where is Kafka?
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

# Topic names
KAFKA_TOPIC=heartbeat-events
KAFKA_DLQ_TOPIC=heartbeat-dlq

# How many fake customers?
CUSTOMER_COUNT=100

# How fast to generate data?
MESSAGES_PER_SECOND=10

# How often should anomalies appear?
ANOMALY_PROBABILITY=0.02  # 2%

# Heart rate thresholds
HR_ANOMALY_LOW=40   # Below this = bradycardia
HR_ANOMALY_HIGH=150 # Above this = tachycardia
HR_CRITICAL_LOW=30  # Below this = critical
HR_CRITICAL_HIGH=180 # Above this = critical

# Batch processing
BATCH_SIZE=100              # Records per database write
BATCH_TIMEOUT_SECONDS=5.0   # Max wait time

# Logging
LOG_LEVEL=INFO
LOG_TO_FILE=true
```

**File:** `src/config.py`

This file loads `.env` and provides typed access:

```python
@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic: str = os.getenv("KAFKA_TOPIC", "heartbeat-events")
```

The `frozen=True` means config objects are immutable - you can't accidentally change them.

---

## The Logging System

**File:** `src/logger.py`

**What it does:** Writes logs to console AND files.

**Log format:**
```
2026-02-18 12:00:00 | INFO     | producer | send:95 | Delivered 100 messages
```

**Log files created:**

```
logs/
├── producer.log           # Producer text logs
├── producer.json.log      # Producer JSON logs (machine-readable)
├── consumer.log           # Consumer text logs
├── consumer.json.log      # Consumer JSON logs
└── errors.log             # All ERROR-level logs from all components
```

**Why JSON logs?**

JSON logs can be parsed by log analysis tools (Elasticsearch, Splunk):

```json
{
    "timestamp": "2026-02-18T12:00:00.123456Z",
    "level": "WARNING",
    "logger": "consumer",
    "function": "_process_message",
    "line": 156,
    "message": "Anomaly: CUST-042 | HR=185 | Type=critical_high | Severity=2",
    "data": {
        "customer_id": "CUST-042",
        "heart_rate": 185,
        "anomaly_type": "critical_high"
    }
}
```

---

## Running the Project Step by Step

### Prerequisites

- Docker Desktop (running)
- Python 3.9+
- PowerShell (Windows)

### Step 1: Start Infrastructure

```powershell
cd "C:\Users\ZainabAbdullai\Desktop\Heartbeat Monitoring System"
docker-compose up -d
```

This starts 7 containers:
1. **zookeeper** - Kafka's coordinator
2. **kafka** - The message broker
3. **schema-registry** - Schema management
4. **postgres** - The database
5. **prometheus** - Metrics storage
6. **grafana** - Metrics visualization
7. **streamlit** - Python dashboard

Wait 30-60 seconds for everything to start.

### Step 2: Verify Services

```powershell
docker-compose ps
```

All should show "healthy" or "running".

### Step 3: Create Virtual Environment

```powershell
.\setup.ps1
```

Or manually:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Step 4: Start Producer

```powershell
cd src
python kafka_producer.py
```

You should see:
```
2026-02-18 12:00:00 | INFO | producer | run:113 | Starting producer: 10.0 msg/sec to heartbeat-events
2026-02-18 12:00:10 | INFO | producer | _delivery_callback:75 | Delivered 100 messages | Rate: 10.0/s | Success: 100.0%
```

### Step 5: Start Consumer (new terminal)

```powershell
cd "C:\Users\ZainabAbdullai\Desktop\Heartbeat Monitoring System"
.\venv\Scripts\Activate.ps1
cd src
python kafka_consumer.py
```

You should see:
```
2026-02-18 12:00:15 | INFO | consumer | run:300 | Consumer started, listening to heartbeat-events
2026-02-18 12:00:20 | INFO | consumer | _flush_batch:245 | Batch written: 100 records | Total: 100 | Throughput: 10.0/s
```

### Step 6: View Dashboards

- **Streamlit:** http://localhost:8501
- **Grafana:** http://localhost:3000 (admin / admin123)
- **Prometheus:** http://localhost:9090

### Step 7: Check Database

```powershell
docker exec -it postgres psql -U postgres -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeat_records;"
```

### Step 8: Stop Everything

```powershell
# Stop producer and consumer with Ctrl+C

# Stop Docker services
docker-compose down
```

---

## Common Questions

### Q: Why Kafka instead of direct database writes?

**A:** Three reasons:

1. **Buffering:** If the database is slow, Kafka absorbs the backlog
2. **Decoupling:** Producer doesn't care if consumer crashes
3. **Replay:** You can reprocess old data by resetting offsets

### Q: What happens if the consumer crashes?

**A:** When it restarts:
1. It reads its last committed offset from Kafka
2. Continues from where it left off
3. Some messages might be processed twice (at-least-once), but none are lost

### Q: Why batch inserts instead of single inserts?

**A:** Performance. Consider:
- 100 single inserts = 100 database round-trips
- 1 batch insert of 100 records = 1 database round-trip

The second approach is ~50x faster.

### Q: What's the difference between consumer and producer offset?

**A:** The **producer** doesn't track offsets - it just sends and forgets. The **consumer** tracks offsets to remember its position.

### Q: Why manual offset commit instead of auto-commit?

**A:** Control. With auto-commit:
1. Consumer reads message
2. Kafka auto-commits "you've read this"
3. Consumer tries to write to database
4. Database crashes
5. Consumer restarts at next message (previous one is lost!)

With manual commit:
1. Consumer reads message
2. Consumer writes to database successfully
3. Consumer manually commits
4. If database crashes before commit, message is re-read

### Q: What triggers the circuit breaker?

**A:** 5 consecutive database failures. After that:
- New writes go to DLQ instead of trying (and failing)
- After 30 seconds, it tries one "test" request
- If successful, circuit closes and normal operation resumes

### Q: How do I add a new anomaly detection rule?

**A:** Edit `src/anomaly_detector.py`:

```python
def detect(self, heart_rate: int) -> AnomalyResult:
    # Add your new rule here
    if heart_rate > 200:
        return AnomalyResult(True, "extreme_tachycardia", 3)
    
    # ... existing rules ...
```

### Q: How do I increase throughput?

**A:** Several options:

1. **Increase producer rate:**
   ```ini
   MESSAGES_PER_SECOND=100
   ```

2. **Add more partitions:**
   ```bash
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic heartbeat-events --partitions 6
   ```

3. **Run multiple consumers** (each handles different partitions)

4. **Increase batch size:**
   ```ini
   BATCH_SIZE=500
   ```

---

## Next Steps

Now that you understand the project:

1. **Experiment:** Change configuration values and observe behavior
2. **Break things:** Stop PostgreSQL and watch the circuit breaker activate
3. **Monitor:** Watch Grafana while producer/consumer are running
4. **Query:** Write SQL queries against the database
5. **Extend:** Add new anomaly detection rules or dashboard panels

Good luck!
