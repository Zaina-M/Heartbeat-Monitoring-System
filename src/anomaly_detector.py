from enum import Enum
from dataclasses import dataclass
from typing import Optional

from config import heartrate_config


class AnomalyType(Enum):
    NONE = None
    BRADYCARDIA = "bradycardia"
    TACHYCARDIA = "tachycardia"
    CRITICAL_LOW = "critical_low"
    CRITICAL_HIGH = "critical_high"


@dataclass
class AnomalyResult:
    is_anomaly: bool
    anomaly_type: Optional[str]
    severity: int  # 0=none, 1=warning, 2=critical


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

    def detect(self, heart_rate: int) -> AnomalyResult:
        if heart_rate <= self.critical_low:
            return AnomalyResult(True, AnomalyType.CRITICAL_LOW.value, 2)

        if heart_rate >= self.critical_high:
            return AnomalyResult(True, AnomalyType.CRITICAL_HIGH.value, 2)

        if heart_rate < self.low_threshold:
            return AnomalyResult(True, AnomalyType.BRADYCARDIA.value, 1)

        if heart_rate > self.high_threshold:
            return AnomalyResult(True, AnomalyType.TACHYCARDIA.value, 1)

        return AnomalyResult(False, AnomalyType.NONE.value, 0)


detector = AnomalyDetector()


if __name__ == "__main__":
    test_rates = [25, 35, 50, 75, 100, 160, 200]
    for rate in test_rates:
        result = detector.detect(rate)
        print(f"HR {rate}: {result}")
