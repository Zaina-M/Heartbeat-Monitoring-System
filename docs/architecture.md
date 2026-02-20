# Architecture Diagram

Create using draw.io, Excalidraw, or similar tool.

## Data Flow

```
+------------------+
|  HeartbeatGenerator  |
|  (data_generator.py) |
+--------+---------+
         |
         | HeartbeatEvent (JSON)
         v
+------------------+
|  HeartbeatProducer   |
|  (kafka_producer.py) |
+--------+---------+
         |
         | Kafka Protocol
         v
+------------------+
|   Kafka Broker       |
|   Topic: heartbeat-events |
+--------+---------+
         |
         | Message polling
         v
+------------------+
|  HeartbeatConsumer   |
|  (kafka_consumer.py) |
|  + AnomalyDetector   |
+--------+---------+
         |
         | Batch INSERT (psycopg2)
         v
+------------------+
|   PostgreSQL         |
|   heartbeat_records  |
+--------+---------+
         |
         | SQL queries
         v
+------------------+
|   Streamlit          |
|   Dashboard          |
+------------------+
```

## Components

### Generator
- Creates synthetic heartbeat data
- Normal distribution for heart rate (mean=80, std=15)
- 2% anomaly injection rate

### Producer
- confluent-kafka Producer
- Partition key: customer_id (ordering guarantee per customer)
- Delivery acknowledgment callbacks
- Graceful shutdown with flush

### Kafka
- Single broker setup (development)
- Topic auto-creation enabled
- Default retention: 7 days

### Consumer
- confluent-kafka Consumer
- Manual offset commit (after DB write)
- Batch processing (100 records or 5s timeout)
- Connection pooling for PostgreSQL

### Database
- PostgreSQL 15
- Indexed on timestamp, customer_id
- Partial index on anomalies

### Dashboard
- Streamlit with auto-refresh
- Real-time metrics
- Per-customer drill-down
- Anomaly highlighting
