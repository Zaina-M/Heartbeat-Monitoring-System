# Simple Threshold-Based Anomaly Detection for Heart Rate Data

from enum import Enum
from dataclasses import dataclass
from typing import Optional

from config import heartrate_config


class AnomalyType(Enum):
    """Types of heart rate anomalies."""
    NONE = None
    BRADYCARDIA = "bradycardia"
    TACHYCARDIA = "tachycardia"
    CRITICAL_LOW = "critical_low"
    CRITICAL_HIGH = "critical_high"


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    anomaly_type: Optional[str]
    severity: int  # 0=normal, 1=warning, 2=critical
    details: Optional[str] = None


class AnomalyDetector:

    
    def __init__(
        self,
        low_threshold: int = heartrate_config.anomaly_low_threshold,
        high_threshold: int = heartrate_config.anomaly_high_threshold,
        critical_low: int = heartrate_config.critical_low,
        critical_high: int = heartrate_config.critical_high,
    ):
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self.critical_low = critical_low
        self.critical_high = critical_high

    def detect(self, heart_rate: int, customer_id: str = None) -> AnomalyResult:
        
       # Detect anomalies using threshold comparison.
       
        # Critical low - immediate danger
        if heart_rate <= self.critical_low:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.CRITICAL_LOW.value,
                severity=2,
                details=f"Critical bradycardia: {heart_rate} BPM <= {self.critical_low}"
            )

        # Critical high - immediate danger
        if heart_rate >= self.critical_high:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.CRITICAL_HIGH.value,
                severity=2,
                details=f"Critical tachycardia: {heart_rate} BPM >= {self.critical_high}"
            )

        # Bradycardia - warning
        if heart_rate < self.low_threshold:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.BRADYCARDIA.value,
                severity=1,
                details=f"Bradycardia: {heart_rate} BPM < {self.low_threshold}"
            )

        # Tachycardia - warning
        if heart_rate > self.high_threshold:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.TACHYCARDIA.value,
                severity=1,
                details=f"Tachycardia: {heart_rate} BPM > {self.high_threshold}"
            )

        # Normal
        return AnomalyResult(
            is_anomaly=False,
            anomaly_type=AnomalyType.NONE.value,
            severity=0
        )


# Global singleton for convenience
detector = AnomalyDetector()


if __name__ == "__main__":
    print("Threshold-Based Anomaly Detection Demo")
    print("=" * 50)
    
    detector = AnomalyDetector()
    
    # Show configured thresholds
    print(f"\nThresholds:")
    print(f"  Critical Low:  <= {detector.critical_low} BPM")
    print(f"  Bradycardia:   <  {detector.low_threshold} BPM")
    print(f"  Normal:        {detector.low_threshold}-{detector.high_threshold} BPM")
    print(f"  Tachycardia:   >  {detector.high_threshold} BPM")
    print(f"  Critical High: >= {detector.critical_high} BPM")
    
    # Test various heart rates
    print("\nTest Results:")
    test_rates = [25, 35, 50, 75, 100, 120, 155, 175, 200]
    
    for rate in test_rates:
        result = detector.detect(rate)
        status = "ANOMALY" if result.is_anomaly else "NORMAL "
        severity = ["OK", "WARN", "CRIT"][result.severity]
        anomaly = result.anomaly_type or "-"
        print(f"  HR {rate:3} BPM: {status} [{severity:4}] {anomaly}")
