import json
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum

from confluent_kafka import Producer

from logger import get_logger


class DLQReason(Enum):
    DESERIALIZATION_ERROR = "deserialization_error"
    VALIDATION_ERROR = "validation_error"
    PROCESSING_ERROR = "processing_error"
    DATABASE_ERROR = "database_error"
    SCHEMA_ERROR = "schema_error"
    UNKNOWN = "unknown"


@dataclass
class DLQMessage:
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str]
    original_value: str
    error_reason: str
    error_message: str
    error_details: Optional[Dict[str, Any]]
    retry_count: int
    timestamp: str
    producer_id: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))


class DeadLetterQueue:
    def __init__(
        self,
        bootstrap_servers: str,
        dlq_topic: str = "heartbeat-dlq",
        max_retries: int = 3,
    ):
        self.dlq_topic = dlq_topic
        self.max_retries = max_retries
        self.logger = get_logger("dlq")

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "dlq-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
        })

        self.logger.info(f"DLQ initialized with topic: {dlq_topic}")

    def _delivery_callback(self, err, msg):
        if err:
            self.logger.error(f"DLQ delivery failed: {err}")
        else:
            self.logger.debug(f"DLQ message delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    def send(
        self,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        original_key: Optional[bytes],
        original_value: bytes,
        reason: DLQReason,
        error: Exception,
        error_details: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
    ) -> bool:
        try:
            dlq_message = DLQMessage(
                original_topic=original_topic,
                original_partition=original_partition,
                original_offset=original_offset,
                original_key=original_key.decode() if original_key else None,
                original_value=original_value.decode(errors="replace"),
                error_reason=reason.value,
                error_message=str(error),
                error_details=error_details,
                retry_count=retry_count,
                timestamp=datetime.now(timezone.utc).isoformat(),
                producer_id="heartbeat-consumer",
            )

            self._producer.produce(
                topic=self.dlq_topic,
                key=original_key,
                value=dlq_message.to_json().encode(),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)

            self.logger.warning(
                f"Message sent to DLQ: topic={original_topic}, "
                f"partition={original_partition}, offset={original_offset}, "
                f"reason={reason.value}",
                extra={"extra_data": {
                    "original_topic": original_topic,
                    "partition": original_partition,
                    "offset": original_offset,
                    "reason": reason.value,
                    "error": str(error),
                }},
            )

            return True

        except Exception as e:
            self.logger.error(f"Failed to send to DLQ: {e}")
            return False

    def flush(self, timeout: float = 10.0):
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            self.logger.warning(f"{remaining} DLQ messages not delivered")

    def close(self):
        self.flush()
        self.logger.info("DLQ closed")
