import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from logger import get_logger

HEARTBEAT_SCHEMA = """{
    "type": "record",
    "name": "HeartbeatEvent",
    "namespace": "com.heartbeat.events",
    "fields": [
        {
            "name": "customer_id",
            "type": "string",
            "doc": "Unique customer identifier"
        },
        {
            "name": "timestamp",
            "type": "string",
            "doc": "ISO 8601 timestamp of the reading"
        },
        {
            "name": "heart_rate",
            "type": "int",
            "doc": "Heart rate in beats per minute"
        }
    ]
}"""

DLQ_SCHEMA = """{
    "type": "record",
    "name": "DLQMessage",
    "namespace": "com.heartbeat.dlq",
    "fields": [
        {"name": "original_topic", "type": "string"},
        {"name": "original_partition", "type": "int"},
        {"name": "original_offset", "type": "long"},
        {"name": "original_key", "type": ["null", "string"], "default": null},
        {"name": "original_value", "type": "string"},
        {"name": "error_reason", "type": "string"},
        {"name": "error_message", "type": "string"},
        {"name": "error_details", "type": ["null", "string"], "default": null},
        {"name": "retry_count", "type": "int"},
        {"name": "timestamp", "type": "string"},
        {"name": "producer_id", "type": "string"}
    ]
}"""


@dataclass
class SchemaConfig:
    registry_url: str = "http://localhost:8081"
    heartbeat_subject: str = "heartbeat-events-value"
    dlq_subject: str = "heartbeat-dlq-value"
    auto_register: bool = True  # Auto-register schemas on startup
    compatibility_level: str = "BACKWARD"  # Schema compatibility mode


class HeartbeatSchemaRegistry:
    """
    Manages Avro schemas for heartbeat events.
    
    Features:
    - Auto-registration of schemas on startup
    - Graceful fallback to JSON when registry unavailable
    - Schema compatibility checking
    """
    
    def __init__(self, config: Optional[SchemaConfig] = None):
        self.config = config or SchemaConfig()
        self.logger = get_logger("schema_registry")

        try:
            self._client = SchemaRegistryClient({"url": self.config.registry_url})
            self._heartbeat_serializer: Optional[AvroSerializer] = None
            self._heartbeat_deserializer: Optional[AvroDeserializer] = None
            self._string_serializer = StringSerializer("utf-8")
            self._initialized = False
            self.logger.info(f"Schema registry client created: {self.config.registry_url}")
        except Exception as e:
            self.logger.warning(f"Schema registry not available: {e}. Using JSON fallback.")
            self._client = None
            self._initialized = False

    def initialize(self) -> bool:
        if self._client is None:
            return False

        if self._initialized:
            return True

        try:
            # Auto-register schema if enabled
            if self.config.auto_register:
                self._register_schemas()
            
            self._heartbeat_serializer = AvroSerializer(
                self._client,
                HEARTBEAT_SCHEMA,
                lambda obj, ctx: obj if isinstance(obj, dict) else obj.__dict__,
            )

            self._heartbeat_deserializer = AvroDeserializer(
                self._client,
                HEARTBEAT_SCHEMA,
            )

            self._initialized = True
            self.logger.info("Schema registry initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize schema registry: {e}")
            return False

    def _register_schemas(self) -> None:
        """Register schemas with the registry if they don't exist."""
        from confluent_kafka.schema_registry import Schema
        
        try:
            # Register heartbeat schema
            heartbeat_schema = Schema(HEARTBEAT_SCHEMA, "AVRO")
            schema_id = self._client.register_schema(
                self.config.heartbeat_subject, 
                heartbeat_schema
            )
            self.logger.info(f"Registered heartbeat schema with ID: {schema_id}")
            
            # Register DLQ schema
            dlq_schema = Schema(DLQ_SCHEMA, "AVRO")
            dlq_id = self._client.register_schema(
                self.config.dlq_subject,
                dlq_schema
            )
            self.logger.info(f"Registered DLQ schema with ID: {dlq_id}")
            
        except Exception as e:
            self.logger.warning(f"Schema registration failed (may already exist): {e}")

    def check_compatibility(self, schema_str: str, subject: str) -> bool:
        """Check if a schema is compatible with the existing schema."""
        from confluent_kafka.schema_registry import Schema
        
        try:
            schema = Schema(schema_str, "AVRO")
            return self._client.test_compatibility(subject, schema)
        except Exception as e:
            self.logger.error(f"Compatibility check failed: {e}")
            return False

    def serialize_heartbeat(self, data: Dict[str, Any], topic: str) -> bytes:
        if self._initialized and self._heartbeat_serializer:
            ctx = SerializationContext(topic, MessageField.VALUE)
            return self._heartbeat_serializer(data, ctx)
        return json.dumps(data).encode("utf-8")

    def deserialize_heartbeat(self, data: bytes, topic: str) -> Dict[str, Any]:
        if self._initialized and self._heartbeat_deserializer:
            ctx = SerializationContext(topic, MessageField.VALUE)
            return self._heartbeat_deserializer(data, ctx)
        return json.loads(data.decode("utf-8"))

    def serialize_key(self, key: str) -> bytes:
        return key.encode("utf-8")

    def get_schema_info(self) -> Dict[str, Any]:
        if self._client is None:
            return {"status": "unavailable", "mode": "json_fallback"}

        try:
            subjects = self._client.get_subjects()
            return {
                "status": "connected",
                "registry_url": self.config.registry_url,
                "subjects": subjects,
                "mode": "avro" if self._initialized else "json_fallback",
            }
        except Exception as e:
            return {"status": "error", "error": str(e), "mode": "json_fallback"}

    @property
    def is_available(self) -> bool:
        return self._initialized


class JsonFallbackSerializer:
    def serialize(self, data: Dict[str, Any]) -> bytes:
        return json.dumps(data).encode("utf-8")

    def deserialize(self, data: bytes) -> Dict[str, Any]:
        return json.loads(data.decode("utf-8"))


schema_registry: Optional[HeartbeatSchemaRegistry] = None
json_serializer = JsonFallbackSerializer()


def get_schema_registry(config: Optional[SchemaConfig] = None) -> HeartbeatSchemaRegistry:
    global schema_registry
    if schema_registry is None:
        schema_registry = HeartbeatSchemaRegistry(config)
        schema_registry.initialize()
    return schema_registry
