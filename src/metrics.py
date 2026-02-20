from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from typing import Optional
import threading


class PrometheusMetrics:
    _instance: Optional["PrometheusMetrics"] = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self.messages_produced = Counter(
            "heartbeat_messages_produced_total",
            "Total messages produced to Kafka",
            ["status"],
        )

        self.messages_consumed = Counter(
            "heartbeat_messages_consumed_total",
            "Total messages consumed from Kafka",
            ["status"],
        )

        self.anomalies_detected = Counter(
            "heartbeat_anomalies_detected_total",
            "Total anomalies detected",
            ["anomaly_type", "severity"],
        )

        self.db_writes = Counter(
            "heartbeat_db_writes_total",
            "Total database write operations",
            ["status"],
        )

        self.dlq_messages = Counter(
            "heartbeat_dlq_messages_total",
            "Messages sent to dead letter queue",
            ["reason"],
        )

        self.producer_queue_size = Gauge(
            "heartbeat_producer_queue_size",
            "Current producer queue size",
        )

        self.consumer_batch_size = Gauge(
            "heartbeat_consumer_batch_size",
            "Current consumer batch size",
        )

        self.consumer_lag = Gauge(
            "heartbeat_consumer_lag",
            "Consumer lag per partition",
            ["partition"],
        )

        self.heart_rate_distribution = Histogram(
            "heartbeat_heart_rate_bpm",
            "Distribution of heart rate values",
            buckets=[30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
        )

        self.db_write_duration = Histogram(
            "heartbeat_db_write_duration_seconds",
            "Database write latency",
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        )

        self.message_processing_duration = Histogram(
            "heartbeat_message_processing_duration_seconds",
            "Message processing latency",
            buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
        )

        self.app_info = Info(
            "heartbeat_app",
            "Application information",
        )

    def start_server(self, port: int = 8000):
        start_http_server(port)

    def set_app_info(self, version: str, environment: str):
        self.app_info.info({"version": version, "environment": environment})


metrics = PrometheusMetrics()
