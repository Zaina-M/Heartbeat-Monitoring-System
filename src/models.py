"""
Pydantic Models for Data Validation

Provides strict validation for all data flowing through the system.
Prevents crashes from malformed data and ensures type safety.
"""

from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
import re


class HeartbeatEvent(BaseModel):
    """
    Validates incoming heartbeat events from producers.
    
    Attributes:
        customer_id: Customer identifier in format CUST-XXX
        timestamp: ISO 8601 formatted timestamp
        heart_rate: Heart rate in BPM (1-300 valid range)
    """
    customer_id: str = Field(
        ..., 
        min_length=8, 
        max_length=20,
        description="Customer ID in format CUST-XXX"
    )
    timestamp: str = Field(
        ..., 
        description="ISO 8601 timestamp"
    )
    heart_rate: int = Field(
        ..., 
        ge=1, 
        le=300,
        description="Heart rate in beats per minute"
    )

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        """Ensure customer_id follows CUST-XXX pattern."""
        pattern = r"^CUST-\d{3,5}$"
        if not re.match(pattern, v):
            raise ValueError(
                f"customer_id must match pattern CUST-XXX (e.g., CUST-001), got: {v}"
            )
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Ensure timestamp is valid ISO 8601 format."""
        try:
            # Parse to validate format
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError(f"timestamp must be valid ISO 8601 format, got: {v}")

    @field_validator("heart_rate")
    @classmethod
    def validate_heart_rate(cls, v: int) -> int:
        """Additional heart rate validation."""
        if v <= 0:
            raise ValueError(f"heart_rate must be positive, got: {v}")
        if v > 300:
            raise ValueError(f"heart_rate exceeds maximum of 300, got: {v}")
        return v

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return self.model_dump()


class ProcessedHeartbeat(BaseModel):
    """
    Represents a heartbeat after anomaly detection processing.
    
    This is the format stored in the database.
    """
    customer_id: str = Field(..., min_length=8, max_length=20)
    timestamp: str
    heart_rate: int = Field(..., ge=1, le=300)
    is_anomaly: bool = False
    anomaly_type: Optional[str] = None
    severity: int = Field(default=0, ge=0, le=3)

    @field_validator("anomaly_type")
    @classmethod
    def validate_anomaly_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate anomaly type is a known value."""
        valid_types = {
            None, "low", "high", "critical_low", "critical_high", 
            "bradycardia", "tachycardia"
        }
        if v is not None and v not in valid_types:
            raise ValueError(f"Unknown anomaly_type: {v}")
        return v

    @model_validator(mode="after")
    def validate_anomaly_consistency(self) -> "ProcessedHeartbeat":
        """Ensure is_anomaly and anomaly_type are consistent."""
        if self.is_anomaly and not self.anomaly_type:
            raise ValueError("is_anomaly=True requires anomaly_type to be set")
        if not self.is_anomaly and self.anomaly_type:
            raise ValueError("is_anomaly=False should not have anomaly_type set")
        return self


class DLQMessage(BaseModel):
    """
    Schema for Dead Letter Queue messages.
    
    Contains all context needed to diagnose and retry failed messages.
    """
    original_topic: str
    original_partition: int = Field(..., ge=0)
    original_offset: int = Field(..., ge=0)
    original_key: Optional[str] = None
    original_value: str
    error_reason: str
    error_message: str
    error_details: Optional[str] = None
    retry_count: int = Field(default=0, ge=0)
    timestamp: str
    producer_id: str


class BatchResult(BaseModel):
    """Result of a batch database write operation."""
    inserted: int = Field(default=0, ge=0)
    skipped: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    duration_ms: float = Field(default=0.0, ge=0)

    @property
    def total(self) -> int:
        return self.inserted + self.skipped + self.failed

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 1.0
        return (self.inserted + self.skipped) / self.total


def validate_heartbeat_event(data: dict) -> HeartbeatEvent:
    """
    Validate and parse a raw dictionary into a HeartbeatEvent.
    
    Args:
        data: Raw dictionary from Kafka message
        
    Returns:
        Validated HeartbeatEvent
        
    Raises:
        ValidationError: If data is invalid
    """
    return HeartbeatEvent.model_validate(data)


def validate_processed_heartbeat(data: dict) -> ProcessedHeartbeat:
    """
    Validate and parse a processed heartbeat record.
    
    Args:
        data: Dictionary with anomaly detection results
        
    Returns:
        Validated ProcessedHeartbeat
        
    Raises:
        ValidationError: If data is invalid
    """
    return ProcessedHeartbeat.model_validate(data)
