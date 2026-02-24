"""
Kafka Consumer for Heartbeat Events
Consumes heartbeat data from Kafka, detects anomalies, and stores in PostgreSQL.
"""
import json
import os
import signal
import time
from typing import List, Optional
from dataclasses import dataclass, field
from contextlib import contextmanager


from psycopg2 import extensions
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from config import kafka_config, postgres_config, batch_config, heartrate_config
from anomaly_detector import AnomalyDetector
from logger import get_logger
from dead_letter_queue import DeadLetterQueue, DLQReason
from schema_registry import get_schema_registry
from models import  validate_heartbeat_event
from backpressure import BackpressureHandler, BackpressureConfig


@dataclass
class ConsumerStats:
    received: int = 0
    processed: int = 0
    duplicates_skipped: int = 0
    batches_written: int = 0
    anomalies: int = 0
    errors: int = 0
    dlq_sent: int = 0
    validation_errors: int = 0
    backpressure_waits: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def throughput(self) -> float:
        elapsed = time.time() - self.start_time
        return self.processed / elapsed if elapsed > 0 else 0.0


class DatabaseWriter:
    """Handles database writes with idempotent upsert logic."""

    def __init__(
        self,
        connection_string: str = postgres_config.connection_string,
        pool_min: int = postgres_config.pool_min,
        pool_max: int = postgres_config.pool_max,
    ):
        self.logger = get_logger("db_writer")
        self._pool = ThreadedConnectionPool(pool_min, pool_max, connection_string)
        self.logger.info(f"Database pool initialized: min={pool_min}, max={pool_max}")

    @contextmanager
    def _get_connection(self):
        """Get connection with explicit transaction isolation."""
        conn = self._pool.getconn()
        try:
            # Set isolation level for consistent reads within transaction
            conn.set_isolation_level(extensions.ISOLATION_LEVEL_READ_COMMITTED)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def write_batch(self, records: List[dict]) -> tuple[int, int]:
        
       # Write batch using upsert (ON CONFLICT DO NOTHING).
       
        if not records:
            return 0, 0

        start_time = time.time()
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                values = [
                    (
                        r["customer_id"],
                        r["timestamp"],
                        r["heart_rate"],
                        r["is_anomaly"],
                        r["anomaly_type"],
                        r.get("severity", 0),
                    )
                    for r in records
                ]
                
               
                # Duplicates (same customer_id + timestamp) are silently skipped
                execute_values(
                    cur,
                    """
                    INSERT INTO heartbeat_records 
                    (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
                    VALUES %s
                    ON CONFLICT (customer_id, timestamp) DO NOTHING
                    """,
                    values,
                )
                
                # rowcount tells us how many were actually inserted
                inserted = cur.rowcount
                skipped = len(records) - inserted

        duration = time.time() - start_time
        
        if skipped > 0:
            self.logger.debug(f"Skipped {skipped} duplicate records")
        
        return inserted, skipped

    def write_batch_with_fallback(self, records: List[dict]) -> tuple[int, int, int]:
        
       # Try batch insert, fall back to individual inserts on failure.
    
        if not records:
            return 0, 0, 0

        try:
            inserted, skipped = self.write_batch(records)
            return inserted, skipped, 0
            
        except Exception as batch_error:
            self.logger.warning(f"Batch insert failed: {batch_error}, trying individual inserts")
            
            inserted = 0
            skipped = 0
            failed = 0
            
            for record in records:
                try:
                    with self._get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO heartbeat_records 
                                (customer_id, timestamp, heart_rate, is_anomaly, anomaly_type, severity)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (customer_id, timestamp) DO NOTHING
                                """,
                                (
                                    record["customer_id"],
                                    record["timestamp"],
                                    record["heart_rate"],
                                    record["is_anomaly"],
                                    record["anomaly_type"],
                                    record.get("severity", 0),
                                )
                            )
                            if cur.rowcount > 0:
                                inserted += 1
                            else:
                                skipped += 1
                                
                except Exception as e:
                    self.logger.error(f"Individual insert failed for {record['customer_id']}: {e}")
                    failed += 1
            
            self.logger.info(f"Individual insert complete: {inserted} inserted, {skipped} skipped, {failed} failed")
            return inserted, skipped, failed

    def close(self):
        self._pool.closeall()
        self.logger.info("Database pool closed")


class HeartbeatConsumer:
  

    def __init__(
        self,
        bootstrap_servers: str = kafka_config.bootstrap_servers,
        topic: str = kafka_config.topic,
        group_id: str = kafka_config.consumer_group,
        batch_size: int = batch_config.size,
        batch_timeout_seconds: float = batch_config.timeout_seconds,
        max_pending_records: int = 1000,
    ):
        self.topic = topic
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout_seconds
        self._running = False
        self.stats = ConsumerStats()
        self.logger = get_logger("consumer")

        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        })

        self._db_writer = DatabaseWriter()
        self._detector = AnomalyDetector(
            low_threshold=heartrate_config.anomaly_low_threshold,
            high_threshold=heartrate_config.anomaly_high_threshold,
            critical_low=heartrate_config.critical_low,
            critical_high=heartrate_config.critical_high,
        )
        self._dlq = DeadLetterQueue(bootstrap_servers, kafka_config.dlq_topic)
        self._schema_registry = get_schema_registry()
        
        # Backpressure handler to prevent memory overflow
        self._backpressure = BackpressureHandler(
            BackpressureConfig(max_pending_records=max_pending_records)
        )

        self._batch: List[dict] = []
        self._pending_messages: List = []
        self._last_flush = time.time()

        self.logger.info(
            f"Consumer initialized: topic={topic}, group={group_id}, "
            f"batch_size={batch_size}, brokers={bootstrap_servers}, "
            f"max_pending={max_pending_records}"
        )

    def _deserialize_message(self, msg) -> Optional[dict]:
        """Deserialize and validate message using Pydantic."""
        try:
            if self._schema_registry.is_available:
                raw_data = self._schema_registry.deserialize_heartbeat(msg.value(), self.topic)
            else:
                raw_data = json.loads(msg.value().decode("utf-8"))
            
            # Validate with Pydantic
            validated = validate_heartbeat_event(raw_data)
            return validated.to_dict()
            
        except ValidationError as e:
            self.stats.validation_errors += 1
            self._send_to_dlq(msg, DLQReason.VALIDATION_ERROR, e, 
                             {"validation_errors": str(e.errors())})
            return None
        except Exception as e:
            self._send_to_dlq(msg, DLQReason.DESERIALIZATION_ERROR, e)
            return None

    def _process_message(self, msg) -> Optional[dict]:
        start_time = time.time()

        data = self._deserialize_message(msg)
        if data is None:
            return None

        try:
            # Pass customer_id for advanced pattern-based anomaly detection
            result = self._detector.detect(data["heart_rate"], customer_id=data["customer_id"])

            if result.is_anomaly:
                self.stats.anomalies += 1

                log_msg = (
                    f"Anomaly: {data['customer_id']} | HR={data['heart_rate']} | "
                    f"Type={result.anomaly_type} | Severity={result.severity}"
                )
                if result.details:
                    log_msg += f" | {result.details}"
                self.logger.warning(log_msg)

            duration = time.time() - start_time

            return {
                "customer_id": data["customer_id"],
                "timestamp": data["timestamp"],
                "heart_rate": data["heart_rate"],
                "is_anomaly": result.is_anomaly,
                "anomaly_type": result.anomaly_type,
                "severity": result.severity,
            }

        except KeyError as e:
            self._send_to_dlq(msg, DLQReason.VALIDATION_ERROR, e, {"missing_field": str(e)})
            return None
        except Exception as e:
            self._send_to_dlq(msg, DLQReason.PROCESSING_ERROR, e)
            return None

    def _send_to_dlq(
        self,
        msg,
        reason: DLQReason,
        error: Exception,
        details: Optional[dict] = None,
    ):
        self._dlq.send(
            original_topic=msg.topic(),
            original_partition=msg.partition(),
            original_offset=msg.offset(),
            original_key=msg.key(),
            original_value=msg.value(),
            reason=reason,
            error=error,
            error_details=details,
        )
        self.stats.dlq_sent += 1
        self.stats.errors += 1

    def _should_flush(self) -> bool:
        if len(self._batch) >= self.batch_size:
            return True
        elapsed = time.time() - self._last_flush
        return elapsed >= self.batch_timeout and len(self._batch) > 0

    def _flush_batch(self) -> None:
        if not self._batch:
            return

        try:
            inserted, skipped, failed = self._db_writer.write_batch_with_fallback(self._batch)
            
            self.stats.processed += inserted
            self.stats.duplicates_skipped += skipped
            self.stats.batches_written += 1

            # Only commit if at least some records were successfully processed
            if inserted > 0 or skipped > 0:
                self._consumer.commit()

            self.logger.info(
                f"Batch written: {inserted} inserted | {skipped} duplicates skipped | "
                f"{failed} failed | Total: {self.stats.processed} | "
                f"Throughput: {self.stats.throughput:.1f}/s"
            )

            # Send failed records to DLQ
            if failed > 0:
                for msg in self._pending_messages[-failed:]:
                    self._send_to_dlq(
                        msg,
                        DLQReason.DATABASE_ERROR,
                        Exception("Individual insert failed after batch failure"),
                    )

        except Exception as e:
            self.logger.error(f"Batch write failed completely: {e}")
            
            # Send entire batch to DLQ
            for msg in self._pending_messages:
                self._send_to_dlq(msg, DLQReason.DATABASE_ERROR, e)
                
        finally:
            # Release backpressure slots for all processed records
            batch_size = len(self._batch)
            if batch_size > 0:
                self._backpressure.release(batch_size)
            
            self._batch = []
            self._pending_messages = []
            self._last_flush = time.time()

    def run(self) -> None:
        self._consumer.subscribe([self.topic])
        self._running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info(f"Consumer started, listening to {self.topic}")

        try:
            while self._running:
                # Check backpressure before polling
                if not self._backpressure.can_accept():
                    self.logger.warning("Backpressure: Waiting for capacity")
                    self.stats.backpressure_waits += 1
                    time.sleep(1.0)
                    if self._should_flush():
                        self._flush_batch()
                    continue
                
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    if self._should_flush():
                        self._flush_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.logger.error(f"Kafka error: {msg.error()}")
                    self.stats.errors += 1
                    continue

                self.stats.received += 1
                
                # Check backpressure before processing
                if not self._backpressure.add(1):
                    self.logger.warning("Backpressure: At capacity, waiting")
                    if not self._backpressure.wait_for_capacity(timeout=5.0):
                        self.stats.backpressure_waits += 1
                        continue
                    self._backpressure.add(1)

                record = self._process_message(msg)
                if record:
                    self._batch.append(record)
                    self._pending_messages.append(msg)
                else:
                    # Release backpressure slot if message wasn't added to batch
                    self._backpressure.release(1)

                if self._should_flush():
                    self._flush_batch()

                if self.stats.received % 500 == 0:
                    self._log_stats()

        except Exception as e:
            self.logger.exception(f"Consumer error: {e}")
            raise
        finally:
            self.close()

    def _log_stats(self):
        bp_status = self._backpressure.get_status()
        self.logger.info(
            f"Stats: received={self.stats.received} | processed={self.stats.processed} | "
            f"duplicates={self.stats.duplicates_skipped} | anomalies={self.stats.anomalies} | "
            f"dlq={self.stats.dlq_sent} | validation_errors={self.stats.validation_errors} | "
            f"backpressure={bp_status['state']} ({bp_status['utilization_pct']:.1f}%) | "
            f"throughput={self.stats.throughput:.1f}/s"
        )

    def _signal_handler(self, signum, frame) -> None:
        self.logger.info(f"Shutdown signal received (signal={signum})")
        self._running = False

    def close(self) -> None:
        self.logger.info("Shutting down consumer...")

        if self._batch:
            self.logger.info(f"Flushing final batch of {len(self._batch)} records")
            self._flush_batch()

        self._dlq.close()
        self._consumer.close()
        self._db_writer.close()

        self.logger.info(
            f"Consumer shutdown complete | Received: {self.stats.received} | "
            f"Processed: {self.stats.processed} | Duplicates: {self.stats.duplicates_skipped} | "
            f"Anomalies: {self.stats.anomalies} | DLQ: {self.stats.dlq_sent}"
        )


if __name__ == "__main__":
    consumer = HeartbeatConsumer()
    consumer.run()
