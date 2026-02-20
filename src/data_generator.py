import random
from datetime import datetime, timezone
from typing import Generator
from dataclasses import dataclass, asdict
from itertools import cycle

from config import generator_config, heartrate_config


@dataclass
class HeartbeatEvent:
    customer_id: str
    timestamp: str
    heart_rate: int

    def to_dict(self) -> dict:
        return asdict(self)


class HeartbeatGenerator:
    """
    Generates realistic heartbeat events for a fixed set of customers.
    
    Uses round-robin customer selection to ensure ALL customers produce
    heartbeat data evenly, eliminating "ghost" customers in dashboards.
    
    Features:
    - Fixed customer pool (CUST-001 to CUST-N)
    - Round-robin rotation guarantees all customers report
    - Realistic heart rate using normal distribution
    - Configurable anomaly injection probability
    
    Example:
        generator = HeartbeatGenerator(customer_count=10)
        for event in generator.stream():
            producer.send(event.to_dict())
    """
    
    def __init__(
        self,
        customer_count: int = generator_config.customer_count,
        anomaly_probability: float = generator_config.anomaly_probability,
    ):
        # Fixed customer pool - only these IDs will ever be generated
        self.customer_count = customer_count
        self.customer_ids = [f"CUST-{i:03d}" for i in range(1, customer_count + 1)]
        self.anomaly_probability = anomaly_probability
        
        # Round-robin iterator ensures every customer gets heartbeats
        self._customer_cycle = cycle(self.customer_ids)
        self._current_index = 0
        
        # Heart rate distribution parameters
        self._hr_mean = (heartrate_config.normal_min + heartrate_config.normal_max) / 2
        self._hr_std = (heartrate_config.normal_max - heartrate_config.normal_min) / 4

    def _get_next_customer(self) -> str:
        """
        Round-robin customer selection.
        
        Cycles through all customers in order, ensuring even distribution.
        After CUST-010, wraps back to CUST-001.
        """
        customer_id = next(self._customer_cycle)
        self._current_index = (self._current_index + 1) % self.customer_count
        return customer_id

    def _generate_normal_heart_rate(self) -> int:
        """Generate realistic heart rate using normal distribution."""
        hr = random.gauss(self._hr_mean, self._hr_std)
        return max(45, min(145, int(hr)))

    def _generate_anomaly_heart_rate(self) -> int:
        """Generate anomalous heart rate (too low or too high)."""
        if random.random() < 0.5:
            # Bradycardia - dangerously low
            return random.randint(20, heartrate_config.anomaly_low_threshold - 1)
        # Tachycardia - dangerously high
        return random.randint(heartrate_config.anomaly_high_threshold + 1, 220)

    def generate_event(self) -> HeartbeatEvent:
        """
        Generate a single heartbeat event.
        
        Uses round-robin customer selection to ensure all customers
        produce data evenly over time.
        """
        # Round-robin instead of random.choice()
        customer_id = self._get_next_customer()
        timestamp = datetime.now(timezone.utc).isoformat()

        if random.random() < self.anomaly_probability:
            heart_rate = self._generate_anomaly_heart_rate()
        else:
            heart_rate = self._generate_normal_heart_rate()

        return HeartbeatEvent(
            customer_id=customer_id,
            timestamp=timestamp,
            heart_rate=heart_rate,
        )

    def stream(self) -> Generator[HeartbeatEvent, None, None]:
        """
        Infinite stream of heartbeat events.
        
        Each customer produces exactly 1 heartbeat per full cycle.
        With 10 customers at 10 msg/sec, each customer reports every second.
        """
        while True:
            yield self.generate_event()

    def reset(self) -> None:
        """Reset the round-robin cycle to start from CUST-001."""
        self._customer_cycle = cycle(self.customer_ids)
        self._current_index = 0


if __name__ == "__main__":
    # Demo: Show that all 10 customers produce heartbeats in order
    generator = HeartbeatGenerator(customer_count=10)
    
    print("Round-robin customer selection demo:")
    print("-" * 50)
    
    for i, event in enumerate(generator.stream()):
        print(f"{i+1:2}. {event.customer_id} | HR: {event.heart_rate:3} BPM")
        if i >= 19:  # Show 2 full cycles
            break
    
    print("-" * 50)
    print("Notice: All 10 customers appear exactly twice in sequence.")

