import json
import signal
import time
from typing import Optional
from dataclasses import dataclass, field

from confluent_kafka import Producer, KafkaError

from config import kafka_config, generator_config, metrics_config
from data_generator import HeartbeatGenerator
from logger import get_logger
from metrics import metrics
from schema_registry import get_schema_registry


@dataclass
class ProducerStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def throughput(self) -> float:
        elapsed = time.time() - self.start_time
        return self.delivered / elapsed if elapsed > 0 else 0.0

    @property
    def success_rate(self) -> float:
        total = self.delivered + self.failed
        return (self.delivered / total * 100) if total > 0 else 0.0


class HeartbeatProducer:
    def __init__(
        self,
        bootstrap_servers: str = kafka_config.bootstrap_servers,
        topic: str = kafka_config.topic,
    ):
        self.topic = topic
        self._running = False
        self.stats = ProducerStats()
        self.logger = get_logger("producer")

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "heartbeat-producer",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
            "linger.ms": 5,
            "batch.size": 16384,
            "compression.type": "snappy",
            "enable.idempotence": True,
        })

        self._schema_registry = get_schema_registry()

        if metrics_config.enabled:
            metrics.start_server(metrics_config.port)
            metrics.set_app_info(version="1.0.0", environment="development")
            self.logger.info(f"Prometheus metrics server started on port {metrics_config.port}")

        self.logger.info(
            f"Producer initialized: topic={topic}, bootstrap_servers={bootstrap_servers}"
        )

    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        if err:
            self.stats.failed += 1
            metrics.messages_produced.labels(status="failed").inc()
            self.logger.error(
                f"Delivery failed: {err}",
                extra={"extra_data": {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "key": msg.key().decode() if msg.key() else None,
                }}
            )
        else:
            self.stats.delivered += 1
            metrics.messages_produced.labels(status="success").inc()

            if self.stats.delivered % 100 == 0:
                self.logger.info(
                    f"Delivered {self.stats.delivered} messages | "
                    f"Rate: {self.stats.throughput:.1f}/s | "
                    f"Success: {self.stats.success_rate:.1f}%"
                )

    def send(self, event: dict) -> None:
        key = event["customer_id"].encode("utf-8")

        if self._schema_registry.is_available:
            value = self._schema_registry.serialize_heartbeat(event, self.topic)
        else:
            value = json.dumps(event).encode("utf-8")

        try:
            self._producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            self.stats.sent += 1
            self._producer.poll(0)

            metrics.producer_queue_size.set(len(self._producer))
            metrics.heart_rate_distribution.observe(event["heart_rate"])

        except BufferError:
            self.logger.warning("Producer queue full, waiting...")
            self._producer.poll(1.0)
            self.send(event)

    def run(self, messages_per_second: float = generator_config.messages_per_second) -> None:
        generator = HeartbeatGenerator()
        interval = 1.0 / messages_per_second
        self._running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info(f"Starting producer: {messages_per_second} msg/sec to {self.topic}")

        try:
            for event in generator.stream():
                if not self._running:
                    break
                self.send(event.to_dict())
                time.sleep(interval)
        except Exception as e:
            self.logger.exception(f"Producer error: {e}")
            raise
        finally:
            self.close()

    def _signal_handler(self, signum, frame) -> None:
        self.logger.info(f"Shutdown signal received (signal={signum})")
        self._running = False

    def close(self) -> None:
        self.logger.info("Flushing remaining messages...")
        remaining = self._producer.flush(timeout=10)

        if remaining > 0:
            self.logger.warning(f"{remaining} messages not delivered")

        self.logger.info(
            f"Producer closed | Sent: {self.stats.sent} | "
            f"Delivered: {self.stats.delivered} | Failed: {self.stats.failed}"
        )


if __name__ == "__main__":
    producer = HeartbeatProducer()
    producer.run()
