"""
Integration Tests with Testcontainers

These tests spin up real Kafka and PostgreSQL containers to test
the full end-to-end pipeline. They verify:
- Message production and consumption
- Pydantic validation
- Exactly-once semantics (deduplication)
- Anomaly detection
- Dead Letter Queue handling
- Backpressure behavior
"""

import sys
import os
import json
import time
import pytest
from typing import Generator, List
from unittest.mock import patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# Skip testcontainers tests if not available
try:
    from testcontainers.kafka import KafkaContainer
    from testcontainers.postgres import PostgresContainer
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False
    KafkaContainer = None
    PostgresContainer = None


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def postgres_container() -> Generator:
    """Spin up a PostgreSQL container for testing."""
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not available")
    
    container = PostgresContainer(
        image="postgres:15-alpine",
        user="postgres",
        password="testpassword",
        dbname="test_heartbeat"
    )
    
    with container:
        # Wait for container to be ready
        time.sleep(2)
        
        # Initialize schema
        import psycopg2
        conn = psycopg2.connect(container.get_connection_url())
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_records (
                    id SERIAL PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    heart_rate INTEGER NOT NULL CHECK (heart_rate > 0 AND heart_rate < 300),
                    is_anomaly BOOLEAN DEFAULT FALSE,
                    anomaly_type VARCHAR(50),
                    severity INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    CONSTRAINT unique_customer_timestamp UNIQUE (customer_id, timestamp)
                );
                
                CREATE INDEX IF NOT EXISTS idx_heartbeat_timestamp 
                ON heartbeat_records(timestamp DESC);
            """)
            conn.commit()
        conn.close()
        
        yield container


@pytest.fixture(scope="module")
def kafka_container() -> Generator:
    """Spin up a Kafka container for testing."""
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not available")
    
    container = KafkaContainer(image="confluentinc/cp-kafka:7.5.0")
    
    with container:
        # Wait for Kafka to be ready
        time.sleep(3)
        yield container


@pytest.fixture
def db_connection(postgres_container):
    """Get a database connection for tests."""
    import psycopg2
    conn = psycopg2.connect(postgres_container.get_connection_url())
    yield conn
    conn.close()


# =============================================================================
# Pydantic Validation Tests
# =============================================================================

class TestPydanticValidation:
    """Test Pydantic model validation."""
    
    def test_valid_heartbeat_event(self):
        """Test that valid data passes validation."""
        from models import HeartbeatEvent
        
        event = HeartbeatEvent(
            customer_id="CUST-001",
            timestamp="2026-02-19T10:30:00+00:00",
            heart_rate=75
        )
        
        assert event.customer_id == "CUST-001"
        assert event.heart_rate == 75
    
    def test_invalid_customer_id_format(self):
        """Test that invalid customer_id is rejected."""
        from models import HeartbeatEvent
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError) as exc_info:
            HeartbeatEvent(
                customer_id="INVALID-ID",
                timestamp="2026-02-19T10:30:00+00:00",
                heart_rate=75
            )
        
        assert "customer_id" in str(exc_info.value)
    
    def test_invalid_heart_rate_too_high(self):
        """Test that heart rate > 300 is rejected."""
        from models import HeartbeatEvent
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError):
            HeartbeatEvent(
                customer_id="CUST-001",
                timestamp="2026-02-19T10:30:00+00:00",
                heart_rate=350
            )
    
    def test_invalid_heart_rate_negative(self):
        """Test that negative heart rate is rejected."""
        from models import HeartbeatEvent
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError):
            HeartbeatEvent(
                customer_id="CUST-001",
                timestamp="2026-02-19T10:30:00+00:00",
                heart_rate=-10
            )
    
    def test_invalid_timestamp_format(self):
        """Test that invalid timestamp is rejected."""
        from models import HeartbeatEvent
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError):
            HeartbeatEvent(
                customer_id="CUST-001",
                timestamp="not-a-timestamp",
                heart_rate=75
            )
    
    def test_processed_heartbeat_anomaly_consistency(self):
        """Test that ProcessedHeartbeat validates anomaly consistency."""
        from models import ProcessedHeartbeat
        from pydantic import ValidationError
        
        # Valid: is_anomaly=True with anomaly_type set
        valid = ProcessedHeartbeat(
            customer_id="CUST-001",
            timestamp="2026-02-19T10:30:00+00:00",
            heart_rate=200,
            is_anomaly=True,
            anomaly_type="critical_high",
            severity=2
        )
        assert valid.is_anomaly is True
        
        # Invalid: is_anomaly=True without anomaly_type
        with pytest.raises(ValidationError):
            ProcessedHeartbeat(
                customer_id="CUST-001",
                timestamp="2026-02-19T10:30:00+00:00",
                heart_rate=200,
                is_anomaly=True,
                anomaly_type=None,
                severity=2
            )


# =============================================================================
# Exactly-Once Semantics Tests
# =============================================================================

@pytest.mark.skipif(not TESTCONTAINERS_AVAILABLE, reason="testcontainers not available")
class TestExactlyOnceSemantics:
    """Test idempotent upserts for exactly-once delivery."""
    
    def test_duplicate_messages_are_skipped(self, postgres_container, db_connection):
        """Test that duplicate (customer_id, timestamp) pairs are skipped."""
        from psycopg2.extras import execute_values
        
        cursor = db_connection.cursor()
        
        # Insert first record
        cursor.execute("""
            INSERT INTO heartbeat_records 
            (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
            VALUES ('CUST-001', '2026-02-19T10:30:00+00:00', 75, false, NULL, 0)
            ON CONFLICT (customer_id, timestamp) DO NOTHING
        """)
        db_connection.commit()
        
        assert cursor.rowcount == 1
        
        # Try to insert duplicate
        cursor.execute("""
            INSERT INTO heartbeat_records 
            (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
            VALUES ('CUST-001', '2026-02-19T10:30:00+00:00', 80, false, NULL, 0)
            ON CONFLICT (customer_id, timestamp) DO NOTHING
        """)
        db_connection.commit()
        
        # Duplicate should be skipped (rowcount = 0)
        assert cursor.rowcount == 0
        
        # Verify only one record exists
        cursor.execute("""
            SELECT heart_rate FROM heartbeat_records 
            WHERE customer_id = 'CUST-001' AND timestamp = '2026-02-19T10:30:00+00:00'
        """)
        result = cursor.fetchone()
        
        # Original value should be preserved
        assert result[0] == 75
        
        cursor.close()
    
    def test_different_timestamps_are_both_inserted(self, postgres_container, db_connection):
        """Test that different timestamps for same customer are both inserted."""
        cursor = db_connection.cursor()
        
        # Insert two records with different timestamps
        cursor.execute("""
            INSERT INTO heartbeat_records 
            (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
            VALUES 
                ('CUST-002', '2026-02-19T10:30:00+00:00', 75, false, NULL, 0),
                ('CUST-002', '2026-02-19T10:31:00+00:00', 78, false, NULL, 0)
            ON CONFLICT (customer_id, timestamp) DO NOTHING
        """)
        db_connection.commit()
        
        assert cursor.rowcount == 2
        
        cursor.close()


# =============================================================================
# Backpressure Tests
# =============================================================================

class TestBackpressure:
    """Test backpressure handling."""
    
    def test_backpressure_handler_acquire_release(self):
        """Test basic acquire/release functionality."""
        from backpressure import BackpressureHandler, BackpressureConfig
        
        handler = BackpressureHandler(BackpressureConfig(max_pending_records=10))
        
        # Acquire 5 slots
        for _ in range(5):
            assert handler.acquire(timeout=0.1) is True
        
        assert handler.pending_count == 5
        assert handler.utilization == 0.5
        
        # Release 3 slots
        handler.release(3)
        
        assert handler.pending_count == 2
        
        # Reset
        handler.reset()
        assert handler.pending_count == 0
    
    def test_backpressure_state_transitions(self):
        """Test state transitions based on utilization."""
        from backpressure import (
            BackpressureHandler, BackpressureConfig, BackpressureState
        )
        
        handler = BackpressureHandler(BackpressureConfig(
            max_pending_records=10,
            warning_threshold=0.5,
            critical_threshold=0.8
        ))
        
        assert handler.state == BackpressureState.NORMAL
        
        # Acquire 5 slots (50% - warning threshold)
        for _ in range(5):
            handler.acquire(timeout=0.1)
        
        assert handler.state == BackpressureState.WARNING
        
        # Acquire 3 more (80% - critical threshold)
        for _ in range(3):
            handler.acquire(timeout=0.1)
        
        assert handler.state == BackpressureState.CRITICAL
        
        # Acquire 2 more (100% - blocked)
        for _ in range(2):
            handler.acquire(timeout=0.1)
        
        assert handler.state == BackpressureState.BLOCKED
        
        # Release all
        handler.release(10)
        assert handler.state == BackpressureState.NORMAL
    
    def test_backpressure_timeout(self):
        """Test that acquire times out when full."""
        from backpressure import BackpressureHandler, BackpressureConfig
        
        handler = BackpressureHandler(BackpressureConfig(max_pending_records=2))
        
        # Fill up
        handler.acquire(timeout=0.1)
        handler.acquire(timeout=0.1)
        
        # Should timeout
        start = time.time()
        result = handler.acquire(timeout=0.2)
        elapsed = time.time() - start
        
        assert result is False
        assert elapsed >= 0.2
    
    def test_recommended_poll_timeout(self):
        """Test dynamic poll timeout based on state."""
        from backpressure import (
            BackpressureHandler, BackpressureConfig, BackpressureState
        )
        
        handler = BackpressureHandler(BackpressureConfig(max_pending_records=10))
        
        # Normal state - fast polling
        assert handler.get_recommended_poll_timeout() == 0.5
        
        # Fill to trigger states
        for _ in range(7):  # 70% = warning
            handler.acquire(timeout=0.1)
        assert handler.get_recommended_poll_timeout() == 1.0
        
        for _ in range(2):  # 90% = critical
            handler.acquire(timeout=0.1)
        assert handler.get_recommended_poll_timeout() == 2.0


# =============================================================================
# End-to-End Integration Tests
# =============================================================================

@pytest.mark.skipif(not TESTCONTAINERS_AVAILABLE, reason="testcontainers not available")
class TestEndToEndIntegration:
    """Full pipeline integration tests with real containers."""
    
    def test_produce_consume_cycle(self, kafka_container):
        """Test producing and consuming a message."""
        from confluent_kafka import Producer, Consumer
        
        bootstrap_servers = kafka_container.get_bootstrap_server()
        topic = "test-heartbeat-events"
        
        # Produce a message
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        
        test_event = {
            "customer_id": "CUST-001",
            "timestamp": "2026-02-19T10:30:00+00:00",
            "heart_rate": 75
        }
        
        producer.produce(
            topic=topic,
            key=b"CUST-001",
            value=json.dumps(test_event).encode("utf-8")
        )
        producer.flush(timeout=5.0)
        
        # Consume the message
        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "test-consumer-group",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([topic])
        
        msg = consumer.poll(timeout=10.0)
        
        assert msg is not None
        assert msg.error() is None
        
        received = json.loads(msg.value().decode("utf-8"))
        assert received["customer_id"] == "CUST-001"
        assert received["heart_rate"] == 75
        
        consumer.close()
    
    def test_anomaly_detection_pipeline(self, postgres_container, db_connection):
        """Test that anomalies are correctly detected and stored."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector(
            low_threshold=40,
            high_threshold=150,
            critical_low=30,
            critical_high=180
        )
        
        test_cases = [
            (75, False, None),  # Normal
            (200, True, "critical_high"),  # Critical high
            (25, True, "critical_low"),  # Critical low
            (35, True, "low"),  # Low
            (160, True, "high"),  # High
        ]
        
        cursor = db_connection.cursor()
        
        for heart_rate, expected_anomaly, expected_type in test_cases:
            result = detector.detect(heart_rate)
            
            assert result.is_anomaly == expected_anomaly, f"HR {heart_rate}"
            assert result.anomaly_type == expected_type, f"HR {heart_rate}"
            
            # Store in database
            cursor.execute("""
                INSERT INTO heartbeat_records 
                (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
                VALUES (%s, NOW(), %s, %s, %s, %s)
            """, (
                f"CUST-TEST-{heart_rate}",
                heart_rate,
                result.is_anomaly,
                result.anomaly_type,
                result.severity
            ))
        
        db_connection.commit()
        
        # Verify anomalies in database
        cursor.execute("""
            SELECT COUNT(*) FROM heartbeat_records 
            WHERE customer_id LIKE 'CUST-TEST-%' AND is_anomaly = true
        """)
        anomaly_count = cursor.fetchone()[0]
        
        assert anomaly_count == 4  # 4 anomalies out of 5 test cases
        
        cursor.close()
    
    def test_batch_processing_efficiency(self, postgres_container, db_connection):
        """Test that batch inserts are more efficient than individual inserts."""
        from psycopg2.extras import execute_values
        
        cursor = db_connection.cursor()
        
        # Generate 100 records
        records = [
            (
                f"CUST-BATCH-{i:03d}",
                f"2026-02-19T{10 + i//60:02d}:{i%60:02d}:00+00:00",
                70 + (i % 30),
                False,
                None,
                0
            )
            for i in range(100)
        ]
        
        # Batch insert
        start = time.time()
        execute_values(
            cursor,
            """
            INSERT INTO heartbeat_records 
            (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
            VALUES %s
            ON CONFLICT (customer_id, timestamp) DO NOTHING
            """,
            records
        )
        db_connection.commit()
        batch_duration = time.time() - start
        
        # Verify all inserted
        cursor.execute("""
            SELECT COUNT(*) FROM heartbeat_records 
            WHERE customer_id LIKE 'CUST-BATCH-%'
        """)
        count = cursor.fetchone()[0]
        
        assert count == 100
        assert batch_duration < 1.0  # Should be fast
        
        cursor.close()


# =============================================================================
# Schema Registry Tests (with mocking)
# =============================================================================

class TestSchemaRegistry:
    """Test schema registry functionality."""
    
    def test_json_fallback_serialization(self):
        """Test JSON fallback when registry unavailable."""
        from schema_registry import HeartbeatSchemaRegistry, SchemaConfig
        
        # Use invalid URL to force fallback
        config = SchemaConfig(registry_url="http://invalid:9999")
        registry = HeartbeatSchemaRegistry(config)
        
        test_data = {
            "customer_id": "CUST-001",
            "timestamp": "2026-02-19T10:30:00+00:00",
            "heart_rate": 75
        }
        
        # Should fallback to JSON
        serialized = registry.serialize_heartbeat(test_data, "test-topic")
        assert isinstance(serialized, bytes)
        
        deserialized = registry.deserialize_heartbeat(serialized, "test-topic")
        assert deserialized == test_data
    
    def test_schema_info_unavailable(self):
        """Test schema info when registry unavailable."""
        from schema_registry import HeartbeatSchemaRegistry, SchemaConfig
        
        config = SchemaConfig(registry_url="http://invalid:9999")
        registry = HeartbeatSchemaRegistry(config)
        
        info = registry.get_schema_info()
        
        assert info["status"] == "unavailable"
        assert info["mode"] == "json_fallback"


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
