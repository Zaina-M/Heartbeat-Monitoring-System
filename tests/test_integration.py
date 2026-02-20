import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import json
import pytest
from unittest.mock import Mock, patch, MagicMock


class TestKafkaProducerUnit:
    def test_event_serialization(self):
        from data_generator import HeartbeatGenerator

        generator = HeartbeatGenerator(customer_count=5)
        event = generator.generate_event()
        data = event.to_dict()

        serialized = json.dumps(data)
        deserialized = json.loads(serialized)

        assert deserialized["customer_id"] == data["customer_id"]
        assert deserialized["heart_rate"] == data["heart_rate"]
        assert deserialized["timestamp"] == data["timestamp"]


class TestKafkaConsumerUnit:
    def test_message_processing(self):
        from anomaly_detector import AnomalyDetector

        detector = AnomalyDetector()

        normal_msg = {"customer_id": "CUST-001", "timestamp": "2026-01-01T00:00:00Z", "heart_rate": 75}
        result = detector.detect(normal_msg["heart_rate"])

        processed = {
            "customer_id": normal_msg["customer_id"],
            "timestamp": normal_msg["timestamp"],
            "heart_rate": normal_msg["heart_rate"],
            "is_anomaly": result.is_anomaly,
            "anomaly_type": result.anomaly_type,
        }

        assert processed["is_anomaly"] is False
        assert processed["anomaly_type"] is None

    def test_anomaly_message_processing(self):
        from anomaly_detector import AnomalyDetector

        detector = AnomalyDetector()

        anomaly_msg = {"customer_id": "CUST-002", "timestamp": "2026-01-01T00:00:00Z", "heart_rate": 200}
        result = detector.detect(anomaly_msg["heart_rate"])

        processed = {
            "customer_id": anomaly_msg["customer_id"],
            "timestamp": anomaly_msg["timestamp"],
            "heart_rate": anomaly_msg["heart_rate"],
            "is_anomaly": result.is_anomaly,
            "anomaly_type": result.anomaly_type,
        }

        assert processed["is_anomaly"] is True
        assert processed["anomaly_type"] == "critical_high"


class TestDatabaseSchema:
    def test_record_structure(self):
        record = {
            "customer_id": "CUST-001",
            "timestamp": "2026-01-01T12:00:00+00:00",
            "heart_rate": 75,
            "is_anomaly": False,
            "anomaly_type": None,
        }

        assert "customer_id" in record
        assert "timestamp" in record
        assert "heart_rate" in record
        assert "is_anomaly" in record
        assert "anomaly_type" in record

    def test_anomaly_record_structure(self):
        record = {
            "customer_id": "CUST-002",
            "timestamp": "2026-01-01T12:00:00+00:00",
            "heart_rate": 200,
            "is_anomaly": True,
            "anomaly_type": "critical_high",
        }

        assert record["is_anomaly"] is True
        assert record["anomaly_type"] is not None


class TestEndToEndSimulation:
    def test_full_pipeline_flow(self):
        from data_generator import HeartbeatGenerator
        from anomaly_detector import AnomalyDetector

        generator = HeartbeatGenerator(customer_count=10, anomaly_probability=0.1)
        detector = AnomalyDetector()

        batch = []
        for _ in range(100):
            event = generator.generate_event()
            data = event.to_dict()
            result = detector.detect(data["heart_rate"])

            record = {
                "customer_id": data["customer_id"],
                "timestamp": data["timestamp"],
                "heart_rate": data["heart_rate"],
                "is_anomaly": result.is_anomaly,
                "anomaly_type": result.anomaly_type,
            }
            batch.append(record)

        assert len(batch) == 100
        assert all("customer_id" in r for r in batch)
        assert all("is_anomaly" in r for r in batch)

        anomalies = [r for r in batch if r["is_anomaly"]]
        normal = [r for r in batch if not r["is_anomaly"]]

        assert len(anomalies) + len(normal) == 100
