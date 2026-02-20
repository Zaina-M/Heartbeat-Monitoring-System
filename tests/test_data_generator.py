import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from data_generator import HeartbeatGenerator, HeartbeatEvent


class TestHeartbeatGenerator:
    def test_event_structure(self):
        generator = HeartbeatGenerator(customer_count=10)
        event = generator.generate_event()

        assert isinstance(event, HeartbeatEvent)
        assert event.customer_id.startswith("CUST-")
        assert isinstance(event.heart_rate, int)
        assert event.timestamp is not None

    def test_customer_id_format(self):
        generator = HeartbeatGenerator(customer_count=100)
        event = generator.generate_event()

        parts = event.customer_id.split("-")
        assert len(parts) == 2
        assert parts[0] == "CUST"
        assert parts[1].isdigit()
        assert len(parts[1]) == 3

    def test_heart_rate_range(self):
        generator = HeartbeatGenerator(customer_count=10, anomaly_probability=0)

        for _ in range(100):
            event = generator.generate_event()
            assert 20 <= event.heart_rate <= 220

    def test_normal_heart_rate_distribution(self):
        generator = HeartbeatGenerator(customer_count=10, anomaly_probability=0)

        rates = [generator.generate_event().heart_rate for _ in range(1000)]
        avg = sum(rates) / len(rates)

        assert 60 <= avg <= 100

    def test_anomaly_generation(self):
        generator = HeartbeatGenerator(customer_count=10, anomaly_probability=1.0)

        event = generator.generate_event()
        assert event.heart_rate < 40 or event.heart_rate > 150

    def test_to_dict(self):
        generator = HeartbeatGenerator(customer_count=10)
        event = generator.generate_event()
        data = event.to_dict()

        assert "customer_id" in data
        assert "timestamp" in data
        assert "heart_rate" in data
        assert isinstance(data, dict)

    def test_stream_generator(self):
        generator = HeartbeatGenerator(customer_count=10)
        stream = generator.stream()

        events = [next(stream) for _ in range(5)]
        assert len(events) == 5
        assert all(isinstance(e, HeartbeatEvent) for e in events)

    def test_customer_count(self):
        generator = HeartbeatGenerator(customer_count=5)
        assert len(generator.customer_ids) == 5

        customer_ids = set()
        for _ in range(100):
            event = generator.generate_event()
            customer_ids.add(event.customer_id)

        assert len(customer_ids) <= 5
