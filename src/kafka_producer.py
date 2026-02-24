import json
import os
import signal
import time
import threading
from typing import Optional
from dataclasses import dataclass, field

from confluent_kafka import Producer, KafkaError

from config import kafka_config, generator_config
from data_generator import HeartbeatGenerator
from logger import get_logger
from schema_registry import get_schema_registry


@dataclass
class ProducerStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    rate_limited: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def throughput(self) -> float:
        elapsed = time.time() - self.start_time
        return self.delivered / elapsed if elapsed > 0 else 0.0

    @property
    def success_rate(self) -> float:
        total = self.delivered + self.failed
        return (self.delivered / total * 100) if total > 0 else 0.0


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter to prevent overwhelming Kafka.
    
    Allows bursts up to bucket_size, then enforces rate limit.
    Thread-safe implementation.
    """
    
    def __init__(self, rate: float, bucket_size: int = 50):
      
        self.rate = rate
        self.bucket_size = bucket_size
        self.tokens = bucket_size
        self.last_update = time.time()
        self._lock = threading.Lock()
        self._enabled = rate > 0
    
    def acquire(self, timeout: float = 1.0) -> bool:
        
        # Acquire a token, blocking if necessary.
      
        if not self._enabled:
            return True
        
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
            # Wait a bit before retrying
            time.sleep(0.01)
        
        return False
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.bucket_size, self.tokens + elapsed * self.rate)
        self.last_update = now
    
    @property
    def available_tokens(self) -> float:
        """Current available tokens."""
        with self._lock:
            self._refill()
            return self.tokens


class HeartbeatProducer:
    def __init__(
        self,
        bootstrap_servers: str = kafka_config.bootstrap_servers,
        topic: str = kafka_config.topic,
        max_rate: float = float(os.getenv("PRODUCER_MAX_RATE", "0")),
        burst_size: int = int(os.getenv("PRODUCER_BURST_SIZE", "50")),
    ):
        self.topic = topic
        self._running = False
        self.stats = ProducerStats()
        self.logger = get_logger("producer")
        
        # Rate limiter to prevent overwhelming Kafka
        self._rate_limiter = TokenBucketRateLimiter(rate=max_rate, bucket_size=burst_size)
        if max_rate > 0:
            self.logger.info(f"Rate limiting enabled: {max_rate} msg/sec, burst={burst_size}")

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

        self.logger.info(
            f"Producer initialized: topic={topic}, bootstrap_servers={bootstrap_servers}"
        )

    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        if err:
            self.stats.failed += 1
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

            if self.stats.delivered % 100 == 0:
                self.logger.info(
                    f"Delivered {self.stats.delivered} messages | "
                    f"Rate: {self.stats.throughput:.1f}/s | "
                    f"Success: {self.stats.success_rate:.1f}%"
                )

    def send(self, event: dict) -> bool:
        # Send event to Kafka with rate limiting.
        
        if not self._rate_limiter.acquire(timeout=0.5):
            self.stats.rate_limited += 1
            if self.stats.rate_limited % 100 == 0:
                self.logger.warning(
                    f"Rate limited {self.stats.rate_limited} messages | "
                    f"Available tokens: {self._rate_limiter.available_tokens:.1f}"
                )
            return False
        
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

            return True

        except BufferError:
            self.logger.warning("Producer queue full, waiting...")
            self._producer.poll(1.0)
            return self.send(event)

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
            f"Delivered: {self.stats.delivered} | Failed: {self.stats.failed} | "
            f"Rate limited: {self.stats.rate_limited}"
        )


if __name__ == "__main__":
    producer = HeartbeatProducer()
    producer.run()
