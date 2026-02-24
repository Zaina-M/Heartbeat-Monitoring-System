import random
from datetime import datetime, timezone
from typing import Generator, List
from dataclasses import dataclass, asdict

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
    
    Uses weighted random selection to simulate realistic usage patterns
    where some customers are more active than others (e.g., athletes vs
    sedentary individuals, 24/7 wearers vs occasional users).
    
    Features:
    - Fixed customer pool (CUST-001 to CUST-N)
    - Weighted random selection (simulates varied device usage)
    - Realistic heart rate using normal distribution
    - Per-customer baseline heart rates (individual variation)
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
        
        # Weighted random selection - simulates realistic usage patterns
        # Some customers report more frequently (active device users)
        # Weights follow a realistic distribution: most users moderate, some heavy, some light
        self._customer_weights = self._generate_realistic_weights()
        
        # Per-customer baseline heart rates (individual physiology)
        # Athletes: lower resting HR (50-65), Sedentary: higher (70-90)
        self._customer_baselines = {
            cid: random.gauss(75, 12) for cid in self.customer_ids
        }
        
        # Heart rate distribution parameters
        self._hr_mean = (heartrate_config.normal_min + heartrate_config.normal_max) / 2
        self._hr_std = (heartrate_config.normal_max - heartrate_config.normal_min) / 4

    def _generate_realistic_weights(self) -> List[float]:
        """
        Generate realistic activity weights using a log-normal distribution.
        
        This simulates real-world device usage patterns:
        - Most users: moderate usage (weight ~1.0)
        - Some power users: high activity (weight 1.5-2.0)
        - Some casual users: low activity (weight 0.3-0.7)
        """
        weights = []
        for _ in range(self.customer_count):
            # Log-normal distribution creates realistic "long tail" of activity
            weight = random.lognormvariate(0, 0.5)  # mu=0, sigma=0.5
            # Clamp to reasonable range
            weight = max(0.2, min(3.0, weight))
            weights.append(weight)
        return weights

    def _get_next_customer(self) -> str:
        """
        Weighted random customer selection.
        
        Selects customers based on their activity weights, creating
        realistic distribution where some customers report more frequently.
        """
        return random.choices(
            self.customer_ids, 
            weights=self._customer_weights,
            k=1
        )[0]

    def _generate_normal_heart_rate(self, customer_id: str) -> int:
        """
        Generate realistic heart rate using normal distribution.
        
        Uses per-customer baseline for individual variation.
        """
        baseline = self._customer_baselines.get(customer_id, self._hr_mean)
        hr = random.gauss(baseline, self._hr_std)
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
        
        Uses weighted random customer selection to simulate realistic
        usage patterns where some customers are more active.
        """
        # Weighted random instead of round-robin
        customer_id = self._get_next_customer()
        timestamp = datetime.now(timezone.utc).isoformat()

        if random.random() < self.anomaly_probability:
            heart_rate = self._generate_anomaly_heart_rate()
        else:
            heart_rate = self._generate_normal_heart_rate(customer_id)

        return HeartbeatEvent(
            customer_id=customer_id,
            timestamp=timestamp,
            heart_rate=heart_rate,
        )

    def stream(self) -> Generator[HeartbeatEvent, None, None]:
        """
        Infinite stream of heartbeat events.
        
        Customers are selected with weighted probability, creating
        realistic distribution where some customers report more frequently.
        """
        while True:
            yield self.generate_event()

    def reset(self) -> None:
        """Reset customer weights (regenerate activity patterns)."""
        self._customer_weights = self._generate_realistic_weights()
        self._customer_baselines = {
            cid: random.gauss(75, 12) for cid in self.customer_ids
        }
    
    def get_customer_stats(self) -> dict:
        """Return customer activity weights and baselines for debugging."""
        return {
            "weights": dict(zip(self.customer_ids, self._customer_weights)),
            "baselines": self._customer_baselines,
        }


if __name__ == "__main__":
    # Demo: Show weighted random selection
    generator = HeartbeatGenerator(customer_count=10)
    
    print("Weighted random customer selection demo:")
    print("-" * 50)
    
    # Show weights
    stats = generator.get_customer_stats()
    print("\nCustomer activity weights:")
    for cid, weight in sorted(stats["weights"].items(), key=lambda x: x[1], reverse=True):
        baseline = stats["baselines"][cid]
        print(f"  {cid}: weight={weight:.2f}, baseline_hr={baseline:.0f}")
    
    # Generate sample events
    print("\nSample events (note: customers with higher weights appear more often):")
    print("-" * 50)
    
    customer_counts = {}
    for i, event in enumerate(generator.stream()):
        customer_counts[event.customer_id] = customer_counts.get(event.customer_id, 0) + 1
        if i < 20:
            print(f"{i+1:2}. {event.customer_id} | HR: {event.heart_rate:3} BPM")
        if i >= 99:
            break
    
    print("-" * 50)
    print("\nCustomer distribution over 100 events:")
    for cid, count in sorted(customer_counts.items()):
        weight = stats["weights"][cid]
        print(f"  {cid}: {count:2} events (weight={weight:.2f})")

