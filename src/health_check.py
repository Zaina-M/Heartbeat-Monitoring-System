import socket
from typing import Dict, Any
import json

import psycopg2
from confluent_kafka.admin import AdminClient, KafkaException

from config import kafka_config, postgres_config
from logger import get_logger

logger = get_logger("health")


def check_kafka() -> Dict[str, Any]:
    try:
        admin = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        topics = list(metadata.topics.keys())

        return {
            "status": "healthy",
            "bootstrap_servers": kafka_config.bootstrap_servers,
            "topics": topics,
            "broker_count": len(metadata.brokers),
        }
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


def check_postgres() -> Dict[str, Any]:
    try:
        conn = psycopg2.connect(
            host=postgres_config.host,
            port=postgres_config.port,
            database=postgres_config.database,
            user=postgres_config.user,
            password=postgres_config.password,
            connect_timeout=5,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.execute("SELECT COUNT(*) FROM heartbeat_records")
            record_count = cur.fetchone()[0]

        conn.close()

        return {
            "status": "healthy",
            "host": postgres_config.host,
            "database": postgres_config.database,
            "record_count": record_count,
        }
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


def check_schema_registry() -> Dict[str, Any]:
    try:
        import requests
        response = requests.get(f"{kafka_config.schema_registry_url}/subjects", timeout=5)

        if response.status_code == 200:
            return {
                "status": "healthy",
                "url": kafka_config.schema_registry_url,
                "subjects": response.json(),
            }
        return {"status": "unhealthy", "error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}


def check_all() -> Dict[str, Any]:
    results = {
        "kafka": check_kafka(),
        "postgres": check_postgres(),
        "schema_registry": check_schema_registry(),
    }

    all_healthy = all(
        r.get("status") == "healthy"
        for r in results.values()
        if r.get("status") != "unavailable"
    )

    results["overall"] = "healthy" if all_healthy else "unhealthy"
    return results


if __name__ == "__main__":
    results = check_all()
    print(json.dumps(results, indent=2))
