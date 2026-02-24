import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _get_env(key: str, default: str) -> str:
    return os.getenv(key, default)


def _get_env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _get_env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _get_env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class KafkaConfig:
    # Bootstrap servers: code defaults to multi-broker pattern
    # In dev, .env overrides to single broker (localhost:29092)
    # In prod, set to: kafka-1:9092,kafka-2:9092,kafka-3:9092
    # Kafka client handles unavailable brokers gracefully
    bootstrap_servers: str = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic: str = _get_env("KAFKA_TOPIC", "heartbeat-events")
    dlq_topic: str = _get_env("KAFKA_DLQ_TOPIC", "heartbeat-dlq")
    consumer_group: str = _get_env("KAFKA_CONSUMER_GROUP", "heartbeat-consumer-group")
    schema_registry_url: str = _get_env("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    use_schema_registry: bool = _get_env_bool("USE_SCHEMA_REGISTRY", False)
    # Replication settings - designed for 3 brokers
    # DEV: Set to 1 via .env (single broker can't replicate)
    # PROD: Set to 3 with min_insync_replicas=2
    replication_factor: int = _get_env_int("KAFKA_REPLICATION_FACTOR", 1)
    min_insync_replicas: int = _get_env_int("KAFKA_MIN_INSYNC_REPLICAS", 1)


@dataclass(frozen=True)
class PostgresConfig:
    host: str = _get_env("POSTGRES_HOST", "localhost")
    port: int = _get_env_int("POSTGRES_PORT", 5432)
    database: str = _get_env("POSTGRES_DB", "heartbeat_db")
    user: str = _get_env("POSTGRES_USER", "postgres")
    password: str = _get_env("POSTGRES_PASSWORD", "postgres123")
    pool_min: int = _get_env_int("POSTGRES_POOL_MIN", 2)
    pool_max: int = _get_env_int("POSTGRES_POOL_MAX", 10)

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class GeneratorConfig:
    customer_count: int = _get_env_int("CUSTOMER_COUNT", 100)
    messages_per_second: float = _get_env_float("MESSAGES_PER_SECOND", 10.0)
    anomaly_probability: float = _get_env_float("ANOMALY_PROBABILITY", 0.02)

    @property
    def message_interval(self) -> float:
        return 1.0 / self.messages_per_second if self.messages_per_second > 0 else 0.1


@dataclass(frozen=True)
class HeartRateConfig:
    normal_min: int = _get_env_int("HR_NORMAL_MIN", 60)
    normal_max: int = _get_env_int("HR_NORMAL_MAX", 100)
    anomaly_low_threshold: int = _get_env_int("HR_ANOMALY_LOW", 40)
    anomaly_high_threshold: int = _get_env_int("HR_ANOMALY_HIGH", 150)
    critical_low: int = _get_env_int("HR_CRITICAL_LOW", 30)
    critical_high: int = _get_env_int("HR_CRITICAL_HIGH", 180)
    mean: int = _get_env_int("HR_MEAN", 75)
    std: int = _get_env_int("HR_STD", 15)


@dataclass(frozen=True)
class BatchConfig:
    size: int = _get_env_int("BATCH_SIZE", 100)
    timeout_seconds: float = _get_env_float("BATCH_TIMEOUT_SECONDS", 5.0)


@dataclass(frozen=True)
class LogConfig:
    level: str = _get_env("LOG_LEVEL", "INFO")
    to_file: bool = _get_env_bool("LOG_TO_FILE", True)
    directory: Path = Path(_get_env("LOG_DIR", "logs"))




kafka_config = KafkaConfig()
postgres_config = PostgresConfig()
generator_config = GeneratorConfig()
heartrate_config = HeartRateConfig()
batch_config = BatchConfig()
log_config = LogConfig()

