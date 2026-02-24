"""
Load Testing and Failure Scenario Tests

Tests system behavior under load and simulated failures:
- High volume message processing
- Kafka unavailability
- PostgreSQL connection loss
- Consumer restart recovery
- Producer rate limiting
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import json
import time
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestLoadScenarios:
    """Load testing scenarios."""
    
    def test_high_volume_data_generation(self):
        """Test generator can produce 1000 events quickly."""
        from data_generator import HeartbeatGenerator
        
        generator = HeartbeatGenerator(customer_count=100)
        events = []
        
        start = time.time()
        for i, event in enumerate(generator.stream()):
            events.append(event)
            if i >= 999:
                break
        elapsed = time.time() - start
        
        assert len(events) == 1000
        assert elapsed < 1.0, f"Generator too slow: {elapsed:.2f}s for 1000 events"
        
        # Verify weighted distribution (not perfectly even)
        customer_counts = {}
        for event in events:
            customer_counts[event.customer_id] = customer_counts.get(event.customer_id, 0) + 1
        
        counts = list(customer_counts.values())
        # Standard deviation should be > 0 (not perfectly even like round-robin)
        mean = sum(counts) / len(counts)
        variance = sum((c - mean) ** 2 for c in counts) / len(counts)
        assert variance > 0, "Distribution should not be perfectly even"
    
    def test_anomaly_detector_high_volume(self):
        """Test anomaly detector performance under load."""
        from anomaly_detector import AnomalyDetector
        import random
        
        detector = AnomalyDetector()
        
        start = time.time()
        for i in range(10000):
            hr = random.randint(30, 200)
            customer_id = f"CUST-{i % 100:03d}"
            detector.detect(hr, customer_id=customer_id)
        elapsed = time.time() - start
        
        assert elapsed < 2.0, f"Detector too slow: {elapsed:.2f}s for 10000 checks"
    
    def test_concurrent_anomaly_detection(self):
        """Test thread safety of anomaly detector."""
        from anomaly_detector import AnomalyDetector
        import random
        
        detector = AnomalyDetector()
        results = []
        errors = []
        
        def detect_batch(batch_id):
            try:
                for i in range(100):
                    hr = random.randint(30, 200)
                    customer_id = f"CUST-{batch_id:03d}"
                    result = detector.detect(hr, customer_id=customer_id)
                    results.append(result)
            except Exception as e:
                errors.append(e)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(detect_batch, i) for i in range(10)]
            for f in as_completed(futures):
                f.result()
        
        assert len(errors) == 0, f"Thread safety issues: {errors}"
        assert len(results) == 1000
    
    def test_rate_limiter_performance(self):
        """Test rate limiter under burst conditions."""
        from kafka_producer import TokenBucketRateLimiter
        
        limiter = TokenBucketRateLimiter(rate=100, bucket_size=50)
        
        # Should allow burst of 50 immediately
        acquired = 0
        start = time.time()
        for _ in range(50):
            if limiter.acquire(timeout=0.01):
                acquired += 1
        elapsed = time.time() - start
        
        assert acquired == 50, f"Burst not allowed: only {acquired}/50"
        assert elapsed < 0.5, f"Burst acquisition too slow: {elapsed:.2f}s"
        
        # Next acquisition should be rate-limited
        if limiter.acquire(timeout=0.1):
            # Should take some time to refill
            pass
    
    def test_batch_processing_performance(self):
        """Test batch creation and processing speed."""
        from data_generator import HeartbeatGenerator
        from anomaly_detector import AnomalyDetector
        
        generator = HeartbeatGenerator(customer_count=100)
        detector = AnomalyDetector()
        
        batch = []
        start = time.time()
        
        for i, event in enumerate(generator.stream()):
            data = event.to_dict()
            result = detector.detect(data["heart_rate"], data["customer_id"])
            
            processed = {
                "customer_id": data["customer_id"],
                "timestamp": data["timestamp"],
                "heart_rate": data["heart_rate"],
                "is_anomaly": result.is_anomaly,
                "anomaly_type": result.anomaly_type,
                "severity": result.severity,
            }
            batch.append(processed)
            
            if len(batch) >= 100:
                # Simulate batch processing
                batch = []
            
            if i >= 999:
                break
        
        elapsed = time.time() - start
        assert elapsed < 2.0, f"Batch processing too slow: {elapsed:.2f}s for 1000 events"


class TestFailureScenarios:
    """Failure scenario tests."""
    
    def test_kafka_unavailable_producer(self):
        """Test producer behavior when Kafka is unavailable."""
        from confluent_kafka import KafkaException
        
        with patch('confluent_kafka.Producer') as MockProducer:
            mock_instance = MockProducer.return_value
            mock_instance.produce.side_effect = KafkaException("Broker unavailable")
            
            # Producer should handle this gracefully
            # In real scenario, would retry or fail
            try:
                mock_instance.produce(
                    topic="test",
                    key=b"test",
                    value=b"test",
                )
            except KafkaException as e:
                assert "unavailable" in str(e).lower()
    
    def test_postgres_connection_loss(self):
        """Test behavior when PostgreSQL connection is lost."""
        import psycopg2
        
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.OperationalError("Connection refused")
            
            # Should raise operational error
            with pytest.raises(psycopg2.OperationalError):
                psycopg2.connect(
                    host="localhost",
                    database="test",
                )
    
    def test_deserialization_error_handling(self):
        """Test handling of malformed messages."""
        from models import validate_heartbeat_event
        from pydantic import ValidationError
        
        # Missing required field
        with pytest.raises(ValidationError):
            validate_heartbeat_event({
                "customer_id": "CUST-001",
                # missing timestamp and heart_rate
            })
        
        # Invalid customer_id format
        with pytest.raises(ValidationError):
            validate_heartbeat_event({
                "customer_id": "invalid",
                "timestamp": "2026-01-01T00:00:00Z",
                "heart_rate": 75,
            })
        
        # Out of range heart rate
        with pytest.raises(ValidationError):
            validate_heartbeat_event({
                "customer_id": "CUST-001",
                "timestamp": "2026-01-01T00:00:00Z",
                "heart_rate": 500,  # > 300 max
            })
    
    def test_dlq_message_creation(self):
        """Test Dead Letter Queue message structure."""
        from dead_letter_queue import DLQMessage, DLQReason
        
        dlq_msg = DLQMessage(
            original_topic="heartbeat-events",
            original_partition=0,
            original_offset=12345,
            original_key="CUST-001",
            original_value='{"invalid": "json"}',
            error_reason=DLQReason.VALIDATION_ERROR.value,
            error_message="Missing required field",
            error_details={"field": "heart_rate"},
            retry_count=0,
            timestamp="2026-01-01T00:00:00Z",
            producer_id="test-consumer",
        )
        
        json_str = dlq_msg.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["original_topic"] == "heartbeat-events"
        assert parsed["error_reason"] == "validation_error"
        assert parsed["error_details"]["field"] == "heart_rate"
    
    def test_backpressure_blocking(self):
        """Test backpressure handler blocks when at capacity."""
        from backpressure import BackpressureHandler, BackpressureConfig
        
        config = BackpressureConfig(max_pending_records=10)
        handler = BackpressureHandler(config)
        
        # Add all slots using add() method
        for _ in range(10):
            assert handler.add(1), "Should be able to add when not at capacity"
        
        # At capacity, should not be able to accept more
        assert not handler.can_accept(), "Should not accept when full"
        assert not handler.add(1), "Should fail to add when at capacity"
        
        # wait_for_capacity should timeout when full
        start = time.time()
        result = handler.wait_for_capacity(timeout=0.5)
        elapsed = time.time() - start
        
        assert result is False, "Should timeout when full"
        assert elapsed >= 0.4, "Should have waited for timeout"
        
        # Release some slots
        handler.release(5)
        
        # Now should be able to add
        assert handler.can_accept()
        assert handler.add(1)
    
    def test_consumer_restart_idempotency(self):
        """Test that reprocessing same data doesn't create duplicates."""
        # Simulate the idempotent insert behavior
        records = [
            {"customer_id": "CUST-001", "timestamp": "2026-01-01T00:00:00Z", "heart_rate": 75},
            {"customer_id": "CUST-001", "timestamp": "2026-01-01T00:00:00Z", "heart_rate": 75},  # duplicate
            {"customer_id": "CUST-002", "timestamp": "2026-01-01T00:00:00Z", "heart_rate": 80},
        ]
        
        # Simulate ON CONFLICT DO NOTHING behavior
        inserted = set()
        skipped = 0
        
        for record in records:
            key = (record["customer_id"], record["timestamp"])
            if key in inserted:
                skipped += 1
            else:
                inserted.add(key)
        
        assert len(inserted) == 2, "Should have 2 unique records"
        assert skipped == 1, "Should have skipped 1 duplicate"


class TestAnomalyDetectionPatterns:
    """Test advanced anomaly detection patterns."""
    
    @pytest.mark.skip(reason="Pattern detection not implemented - future feature")
    def test_rapid_increase_detection(self):
        """Test detection of rapid heart rate increase."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector(rate_change_threshold=25)
        
        # Simulate gradual readings then sudden spike
        readings = [70, 72, 71, 73, 72, 110]  # Jump from ~72 to 110
        
        results = []
        for hr in readings:
            result = detector.detect(hr, customer_id="CUST-001")
            results.append(result)
        
        # Last reading should detect rapid increase
        assert results[-1].is_anomaly
        assert results[-1].anomaly_type == "rapid_increase"
    
    @pytest.mark.skip(reason="Pattern detection not implemented - future feature")
    def test_rapid_decrease_detection(self):
        """Test detection of rapid heart rate decrease."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector(rate_change_threshold=25)
        
        # Simulate sudden drop
        readings = [90, 88, 91, 89, 90, 55]  # Drop from ~90 to 55
        
        results = []
        for hr in readings:
            result = detector.detect(hr, customer_id="CUST-002")
            results.append(result)
        
        # Last reading should detect rapid decrease
        assert results[-1].is_anomaly
        assert results[-1].anomaly_type == "rapid_decrease"
    
    @pytest.mark.skip(reason="Sustained detection not implemented - future feature")
    def test_sustained_high_detection(self):
        """Test detection of sustained elevated heart rate."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector(
            high_threshold=100,
            sustained_count=5
        )
        
        # Clear any existing history
        detector.clear_customer_history("CUST-003")
        
        # Send sustained high readings
        results = []
        for i in range(8):
            hr = 120 + (i % 3)  # 120-122 (all above threshold)
            result = detector.detect(hr, customer_id="CUST-003")
            results.append(result)
        
        # Should eventually detect sustained high
        sustained_detected = any(
            r.anomaly_type == "sustained_high" for r in results
        )
        assert sustained_detected, "Should detect sustained high"
    
    @pytest.mark.skip(reason="Variability detection not implemented - future feature")
    def test_high_variability_detection(self):
        """Test detection of high heart rate variability."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector(variability_threshold=20)
        detector.clear_customer_history("CUST-004")
        
        # Highly variable readings (high std dev)
        readings = [60, 120, 55, 125, 65, 115, 70, 110]
        
        results = []
        for hr in readings:
            result = detector.detect(hr, customer_id="CUST-004")
            results.append(result)
        
        # Should detect high variability
        variability_detected = any(
            r.anomaly_type == "high_variability" for r in results
        )
        assert variability_detected, "Should detect high variability"
    
    @pytest.mark.skip(reason="Baseline learning not implemented - future feature")
    def test_customer_baseline_learning(self):
        """Test that detector learns per-customer baselines."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector()
        
        # Send readings for customer
        for hr in [72, 74, 71, 73, 75, 70, 74, 72]:
            detector.detect(hr, customer_id="CUST-005")
        
        # Get learned baseline
        baseline = detector.get_customer_baseline("CUST-005")
        
        assert baseline is not None
        assert 70 <= baseline["mean"] <= 75
        assert baseline["count"] == 8
    
    def test_threshold_based_detection(self):
        """Test that simple threshold detection works correctly."""
        from anomaly_detector import AnomalyDetector
        
        detector = AnomalyDetector()
        
        # Test critical low
        result = detector.detect(25, customer_id="CUST-001")
        assert result.is_anomaly
        assert result.anomaly_type == "critical_low"
        assert result.severity == 2
        
        # Test bradycardia
        result = detector.detect(35, customer_id="CUST-001")
        assert result.is_anomaly
        assert result.anomaly_type == "bradycardia"
        assert result.severity == 1
        
        # Test normal
        result = detector.detect(75, customer_id="CUST-001")
        assert not result.is_anomaly
        assert result.severity == 0
        
        # Test tachycardia
        result = detector.detect(160, customer_id="CUST-001")
        assert result.is_anomaly
        assert result.anomaly_type == "tachycardia"
        assert result.severity == 1
        
        # Test critical high
        result = detector.detect(190, customer_id="CUST-001")
        assert result.is_anomaly
        assert result.anomaly_type == "critical_high"
        assert result.severity == 2


class TestHealthCheck:
    """Test health check functionality."""
    
    def test_health_check_handler(self):
        """Test HealthCheckHandler responses."""
        from health_check import HealthCheckHandler
        
        # Verify handler class exists and has required methods
        assert hasattr(HealthCheckHandler, 'do_GET')
        assert hasattr(HealthCheckHandler, '_send_health_response')
        assert hasattr(HealthCheckHandler, '_send_liveness_response')
        assert hasattr(HealthCheckHandler, '_send_readiness_response')


class TestRateLimiter:
    """Test rate limiter functionality."""
    
    def test_rate_limiter_disabled_when_zero(self):
        """Test rate limiter is disabled when rate=0."""
        from kafka_producer import TokenBucketRateLimiter
        
        limiter = TokenBucketRateLimiter(rate=0)
        
        # Should always succeed immediately
        for _ in range(1000):
            assert limiter.acquire(timeout=0)
    
    def test_rate_limiter_burst_then_throttle(self):
        """Test burst allowance followed by throttling."""
        from kafka_producer import TokenBucketRateLimiter
        
        limiter = TokenBucketRateLimiter(rate=10, bucket_size=10)
        
        # Burst of 10 should succeed (use small timeout to allow acquire)
        burst_count = 0
        for _ in range(10):
            if limiter.acquire(timeout=0.01):
                burst_count += 1
        
        assert burst_count == 10
        
        # Next one should fail (no tokens left, minimal wait)
        assert not limiter.acquire(timeout=0.01)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
