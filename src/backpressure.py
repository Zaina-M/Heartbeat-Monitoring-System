"""
Backpressure Handler for Kafka Consumer

Prevents memory overflow when the consumer can process messages faster
than the database can write them. Uses a semaphore-based approach to
limit the number of pending records.
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum

from logger import get_logger


class BackpressureState(Enum):
    """Current state of the backpressure system."""
    NORMAL = "normal"
    WARNING = "warning"  # 70-90% capacity
    CRITICAL = "critical"  # 90%+ capacity
    BLOCKED = "blocked"  # 100% capacity, blocking new messages


@dataclass
class BackpressureConfig:
    """Configuration for backpressure handling."""
    max_pending_records: int = 1000  # Maximum records waiting for DB write
    warning_threshold: float = 0.70  # 70% capacity triggers warning
    critical_threshold: float = 0.90  # 90% capacity triggers critical
    block_timeout_seconds: float = 30.0  # Max time to wait when blocked
    check_interval_seconds: float = 1.0  # How often to check pressure


@dataclass
class BackpressureMetrics:
    """Metrics for monitoring backpressure state."""
    current_pending: int = 0
    max_pending_reached: int = 0
    times_blocked: int = 0
    total_wait_time_seconds: float = 0.0
    messages_dropped: int = 0
    state_changes: int = 0
    last_state_change: float = field(default_factory=time.time)

    @property
    def utilization(self) -> float:
        """Current utilization as a percentage."""
        return self.current_pending / max(self.max_pending_reached, 1) * 100


class BackpressureHandler:
    """
    Controls message flow to prevent memory overflow.
    
    When the pending record count exceeds thresholds:
    - WARNING (70%): Logs warning, continues processing
    - CRITICAL (90%): Starts throttling, increases polling delays
    - BLOCKED (100%): Blocks new messages until capacity frees up
    
    Usage:
        handler = BackpressureHandler(max_pending=1000)
        
        # Before adding to batch
        if handler.acquire():
            batch.append(record)
        
        # After successful DB write
        handler.release(count=100)
    """

    def __init__(self, config: Optional[BackpressureConfig] = None):
        self.config = config or BackpressureConfig()
        self.logger = get_logger("backpressure")
        
        self._semaphore = threading.Semaphore(self.config.max_pending_records)
        self._pending_count = 0
        self._lock = threading.Lock()
        self._state = BackpressureState.NORMAL
        self._metrics = BackpressureMetrics()
        self._on_state_change: Optional[Callable[[BackpressureState], Any]] = None
        
        self.logger.info(
            f"Backpressure handler initialized: max_pending={self.config.max_pending_records}, "
            f"warning={self.config.warning_threshold:.0%}, critical={self.config.critical_threshold:.0%}"
        )

    @property
    def state(self) -> BackpressureState:
        """Current backpressure state."""
        return self._state

    @property
    def metrics(self) -> BackpressureMetrics:
        """Current backpressure metrics."""
        return self._metrics

    @property
    def pending_count(self) -> int:
        """Number of records currently pending."""
        return self._pending_count

    @property
    def utilization(self) -> float:
        """Current capacity utilization (0.0 to 1.0)."""
        return self._pending_count / self.config.max_pending_records

    def on_state_change(self, callback: Callable[[BackpressureState], Any]) -> None:
        """Register a callback for state changes."""
        self._on_state_change = callback

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a slot for a new pending record.
        
        Args:
            timeout: Max seconds to wait. None uses config default.
            
        Returns:
            True if slot acquired, False if timed out.
        """
        timeout = timeout or self.config.block_timeout_seconds
        start_time = time.time()
        
        acquired = self._semaphore.acquire(timeout=timeout)
        
        wait_time = time.time() - start_time
        
        with self._lock:
            if acquired:
                self._pending_count += 1
                self._metrics.current_pending = self._pending_count
                self._metrics.max_pending_reached = max(
                    self._metrics.max_pending_reached, self._pending_count
                )
                
                if wait_time > 0.1:  # More than 100ms wait
                    self._metrics.total_wait_time_seconds += wait_time
                    self._metrics.times_blocked += 1
                
                self._update_state()
            else:
                self._metrics.messages_dropped += 1
                self.logger.warning(
                    f"Backpressure: Message dropped after {timeout}s timeout. "
                    f"Pending: {self._pending_count}/{self.config.max_pending_records}"
                )
        
        return acquired

    def acquire_batch(self, count: int, timeout: Optional[float] = None) -> int:
        """
        Acquire slots for multiple records.
        
        Args:
            count: Number of slots requested
            timeout: Max seconds to wait per slot
            
        Returns:
            Number of slots actually acquired
        """
        acquired = 0
        for _ in range(count):
            if self.acquire(timeout=timeout):
                acquired += 1
            else:
                break
        return acquired

    def release(self, count: int = 1) -> None:
        """
        Release slots after records are processed.
        
        Args:
            count: Number of slots to release
        """
        with self._lock:
            actual_release = min(count, self._pending_count)
            self._pending_count -= actual_release
            self._metrics.current_pending = self._pending_count
            
            for _ in range(actual_release):
                self._semaphore.release()
            
            self._update_state()

    def _update_state(self) -> None:
        """Update the backpressure state based on current utilization."""
        utilization = self.utilization
        old_state = self._state
        
        if utilization >= 1.0:
            new_state = BackpressureState.BLOCKED
        elif utilization >= self.config.critical_threshold:
            new_state = BackpressureState.CRITICAL
        elif utilization >= self.config.warning_threshold:
            new_state = BackpressureState.WARNING
        else:
            new_state = BackpressureState.NORMAL
        
        if new_state != old_state:
            self._state = new_state
            self._metrics.state_changes += 1
            self._metrics.last_state_change = time.time()
            
            log_msg = (
                f"Backpressure state: {old_state.value} -> {new_state.value} "
                f"(utilization: {utilization:.1%}, pending: {self._pending_count})"
            )
            
            if new_state in (BackpressureState.CRITICAL, BackpressureState.BLOCKED):
                self.logger.warning(log_msg)
            elif new_state == BackpressureState.WARNING:
                self.logger.info(log_msg)
            else:
                self.logger.debug(log_msg)
            
            if self._on_state_change:
                try:
                    self._on_state_change(new_state)
                except Exception as e:
                    self.logger.error(f"State change callback failed: {e}")

    def get_recommended_poll_timeout(self) -> float:
        """
        Get recommended Kafka poll timeout based on backpressure state.
        
        Returns longer timeouts when under pressure to slow down consumption.
        """
        if self._state == BackpressureState.BLOCKED:
            return 5.0  # Wait longer to give DB time
        elif self._state == BackpressureState.CRITICAL:
            return 2.0
        elif self._state == BackpressureState.WARNING:
            return 1.0
        return 0.5  # Normal: fast polling

    def should_skip_poll(self) -> bool:
        """
        Check if we should skip polling due to extreme backpressure.
        
        Returns:
            True if consumer should pause polling
        """
        return self._state == BackpressureState.BLOCKED

    def reset(self) -> None:
        """Reset the backpressure handler to initial state."""
        with self._lock:
            # Release all pending
            for _ in range(self._pending_count):
                self._semaphore.release()
            
            self._pending_count = 0
            self._state = BackpressureState.NORMAL
            self._metrics = BackpressureMetrics()
        
        self.logger.info("Backpressure handler reset")

    def get_status(self) -> dict:
        """Get current status as a dictionary."""
        return {
            "state": self._state.value,
            "pending_count": self._pending_count,
            "max_capacity": self.config.max_pending_records,
            "utilization_percent": self.utilization * 100,
            "metrics": {
                "max_pending_reached": self._metrics.max_pending_reached,
                "times_blocked": self._metrics.times_blocked,
                "total_wait_seconds": self._metrics.total_wait_time_seconds,
                "messages_dropped": self._metrics.messages_dropped,
                "state_changes": self._metrics.state_changes,
            }
        }


class AsyncBackpressureHandler:
    """
    Async version of BackpressureHandler for asyncio-based consumers.
    """
    
    def __init__(self, config: Optional[BackpressureConfig] = None):
        self.config = config or BackpressureConfig()
        self.logger = get_logger("async_backpressure")
        self._semaphore = asyncio.Semaphore(self.config.max_pending_records)
        self._pending_count = 0
        self._state = BackpressureState.NORMAL

    async def acquire(self) -> bool:
        """Acquire a slot asynchronously."""
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self.config.block_timeout_seconds
            )
            self._pending_count += 1
            return True
        except asyncio.TimeoutError:
            self.logger.warning("Async backpressure: acquisition timed out")
            return False

    def release(self, count: int = 1) -> None:
        """Release slots."""
        for _ in range(min(count, self._pending_count)):
            self._semaphore.release()
            self._pending_count -= 1
