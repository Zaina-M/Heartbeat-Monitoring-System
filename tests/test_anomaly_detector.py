import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from anomaly_detector import AnomalyDetector, AnomalyType, AnomalyResult


class TestAnomalyDetector:
    @pytest.fixture
    def detector(self):
        return AnomalyDetector(
            low_threshold=40,
            high_threshold=150,
            critical_low=30,
            critical_high=180,
        )

    def test_normal_heart_rate(self, detector):
        result = detector.detect(75)

        assert result.is_anomaly is False
        assert result.anomaly_type is None
        assert result.severity == 0

    def test_bradycardia(self, detector):
        result = detector.detect(35)

        assert result.is_anomaly is True
        assert result.anomaly_type == "bradycardia"
        assert result.severity == 1

    def test_tachycardia(self, detector):
        result = detector.detect(160)

        assert result.is_anomaly is True
        assert result.anomaly_type == "tachycardia"
        assert result.severity == 1

    def test_critical_low(self, detector):
        result = detector.detect(25)

        assert result.is_anomaly is True
        assert result.anomaly_type == "critical_low"
        assert result.severity == 2

    def test_critical_high(self, detector):
        result = detector.detect(200)

        assert result.is_anomaly is True
        assert result.anomaly_type == "critical_high"
        assert result.severity == 2

    def test_boundary_low_threshold(self, detector):
        result_below = detector.detect(39)
        result_at = detector.detect(40)

        assert result_below.is_anomaly is True
        assert result_at.is_anomaly is False

    def test_boundary_high_threshold(self, detector):
        result_at = detector.detect(150)
        result_above = detector.detect(151)

        assert result_at.is_anomaly is False
        assert result_above.is_anomaly is True

    def test_boundary_critical_low(self, detector):
        result_at = detector.detect(30)
        result_above = detector.detect(31)

        assert result_at.is_anomaly is True
        assert result_at.anomaly_type == "critical_low"
        assert result_above.anomaly_type == "bradycardia"

    def test_boundary_critical_high(self, detector):
        result_at = detector.detect(180)
        result_below = detector.detect(179)

        assert result_at.is_anomaly is True
        assert result_at.anomaly_type == "critical_high"
        assert result_below.anomaly_type == "tachycardia"

    def test_custom_thresholds(self):
        detector = AnomalyDetector(
            low_threshold=50,
            high_threshold=120,
            critical_low=40,
            critical_high=140,
        )

        assert detector.detect(130).is_anomaly is True
        assert detector.detect(45).is_anomaly is True

    def test_result_dataclass(self, detector):
        result = detector.detect(75)

        assert isinstance(result, AnomalyResult)
        assert hasattr(result, 'is_anomaly')
        assert hasattr(result, 'anomaly_type')
        assert hasattr(result, 'severity')
