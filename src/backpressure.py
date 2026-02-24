"""
Simple Backpressure Handler for Kafka Consumer

Prevents memory overflow by limiting the number of pending records
waiting to be written to the database.
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional
from enum import Enum

from logger import get_logger


class BackpressureState(Enum):
    """Current state of the backpressure system."""
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class BackpressureConfig:
    """Configuration for backpressure handling."""
    max_pending_records: int = 1000
    warning_threshold: float = 0.70
    critical_threshold: float = 0.90


class BackpressureHandler:
    """
    Simple backpressure handler using a counter with limits.
    
    Usage:
        handler = BackpressureHandler(max_pending=1000)
        
        # Before adding to batch
        if handler.can_accept():
            handler.add(1)
            batch.append(record)
        
        # After successful DB write
        handler.release(count=100)
    """

    def __init__(self, config: Optional[BackpressureConfig] = None):
        self.config = config or BackpressureConfig()
        self.logger = get_logger("backpressure")
        self._pending_count = 0
        self._lock = threading.Lock()
        
        self.logger.info(f"Backpressure handler initialized: max={self.config.max_pending_records}")

    @property
    def pending_count(self) -> int:
        """Number of records currently pending."""
        return self._pending_count

    @property
    def utilization(self) -> float:
        """Current capacity utilization (0.0 to 1.0)."""
        return self._pending_count / self.config.max_pending_records

    @property
    def state(self) -> BackpressureState:
        """Current backpressure state based on utilization."""
        util = self.utilization
        if util >= self.config.critical_threshold:
            return BackpressureState.CRITICAL
        elif util >= self.config.warning_threshold:
            return BackpressureState.WARNING
        return BackpressureState.NORMAL

    def can_accept(self) -> bool:
        """Check if we can accept more records."""
        return self._pending_count < self.config.max_pending_records

    def add(self, count: int = 1) -> bool:
        """
        Add pending records.
        
        Returns:
            True if added, False if at capacity.
        """
        with self._lock:
            if self._pending_count + count <= self.config.max_pending_records:
                self._pending_count += count
                return True
            return False

    def release(self, count: int = 1) -> None:
        """Release slots after records are processed."""
        with self._lock:
            self._pending_count = max(0, self._pending_count - count)

    def wait_for_capacity(self, timeout: float = 30.0) -> bool:
        """
        Wait until there's capacity available.
        
        Args:
            timeout: Max seconds to wait
            
        Returns:
            True if capacity available, False if timed out
        """
        start = time.time()
        while not self.can_accept():
            if time.time() - start > timeout:
                self.logger.warning(f"Backpressure: timed out waiting for capacity")
                return False
            time.sleep(0.1)
        return True

    def get_status(self) -> dict:
        """Get current status."""
        return {
            "state": self.state.value,
            "pending": self._pending_count,
            "max": self.config.max_pending_records,
            "utilization_pct": round(self.utilization * 100, 1),
        }
