import threading
from typing import Dict, Any, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

import psycopg2
from confluent_kafka.admin import AdminClient

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


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints."""
    
    # Class-level attributes for component stats
    component_stats: Dict[str, Any] = {}
    
    def do_GET(self):
        """Handle GET requests for health endpoints."""
        if self.path == "/health":
            self._send_health_response()
        elif self.path == "/health/live":
            self._send_liveness_response()
        elif self.path == "/health/ready":
            self._send_readiness_response()
        elif self.path == "/health/stats":
            self._send_stats_response()
        else:
            self._send_not_found()
    
    def _send_health_response(self):
        """Full health check of all dependencies."""
        results = check_all()
        status_code = 200 if results["overall"] == "healthy" else 503
        self._send_json_response(results, status_code)
    
    def _send_liveness_response(self):
        """Simple liveness check - is the process running?"""
        self._send_json_response({"status": "alive"}, 200)
    
    def _send_readiness_response(self):
        """Readiness check - can the service handle traffic?"""
        # Check critical dependencies only
        kafka_ok = check_kafka().get("status") == "healthy"
        postgres_ok = check_postgres().get("status") == "healthy"
        
        if kafka_ok and postgres_ok:
            self._send_json_response({"status": "ready"}, 200)
        else:
            self._send_json_response({
                "status": "not_ready",
                "kafka": "ok" if kafka_ok else "failed",
                "postgres": "ok" if postgres_ok else "failed",
            }, 503)
    
    def _send_stats_response(self):
        """Return component statistics."""
        self._send_json_response(self.component_stats, 200)
    
    def _send_json_response(self, data: Dict, status_code: int):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def _send_not_found(self):
        """Send 404 response."""
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "Not found"}).encode())
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


class HealthCheckServer:
    """
    HTTP server for health check endpoints.
    
    Endpoints:
    - /health - Full health check of all dependencies
    - /health/live - Liveness probe (is process running?)
    - /health/ready - Readiness probe (can handle traffic?)
    - /health/stats - Component statistics
    """
    
    def __init__(self, port: int = 8080):
        self.port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.logger = get_logger("health_server")
    
    def start(self) -> None:
        """Start health check server in background thread."""
        try:
            self._server = HTTPServer(("0.0.0.0", self.port), HealthCheckHandler)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            self.logger.info(f"Health check server started on port {self.port}")
        except OSError as e:
            self.logger.error(f"Could not start health server: {e}")
    
    def stop(self) -> None:
        """Stop health check server."""
        if self._server:
            self._server.shutdown()
            self.logger.info("Health check server stopped")
    
    def update_stats(self, stats: Dict[str, Any]) -> None:
        """Update component statistics for /health/stats endpoint."""
        HealthCheckHandler.component_stats = stats


if __name__ == "__main__":
    results = check_all()
    print(json.dumps(results, indent=2))
