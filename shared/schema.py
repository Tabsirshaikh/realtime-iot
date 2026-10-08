"""
Day-1 Contract: Shared message schema and topic definitions for IoT Stream Processing.

This module is the single source of truth (frozen contract) imported by:
- Person A (Stream side: Simulator & Stream Processor)
- Person B (Storage & Serving side: Postgres/Delta Sinks, Analytics, Grafana)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class Topics(str, Enum):
    """Kafka/Redpanda topics defined in the Day-1 contract."""
    RAW = "sensors.raw"
    CLEAN = "sensors.clean"
    DEAD_LETTER = "sensors.dead_letter"
    AGG_1M = "sensors.agg_1m"
    ALERTS = "sensors.alerts"


class AnomalyType(str, Enum):
    """Anomaly categories tracked by the stream intelligence layer."""
    TEMP_SPIKE = "temperature_spike"
    POWER_SURGE = "power_surge"
    SENSOR_STUCK = "sensor_stuck"
    DEVICE_SILENT = "device_silent"


@dataclass
class SensorReading:
    """Core IoT sensor reading schema emitted by smart building devices."""
    device_id: str
    building: str
    floor: int
    room: str
    event_ts: str  # ISO 8601 UTC string (e.g. 2026-10-08T12:00:00.000Z)
    ingest_seq: int
    temperature_c: float
    humidity_pct: float
    power_kw: float
    occupancy: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SensorReading:
        return cls(
            device_id=str(data["device_id"]),
            building=str(data["building"]),
            floor=int(data["floor"]),
            room=str(data["room"]),
            event_ts=str(data["event_ts"]),
            ingest_seq=int(data["ingest_seq"]),
            temperature_c=float(data["temperature_c"]),
            humidity_pct=float(data["humidity_pct"]),
            power_kw=float(data["power_kw"]),
            occupancy=int(data["occupancy"]),
        )

    @classmethod
    def from_json(cls, json_str: str) -> SensorReading:
        return cls.from_dict(json.loads(json_str))

    @property
    def parsed_event_ts(self) -> datetime:
        """Parse event_ts into UTC datetime object."""
        # Handle ISO with or without Z
        ts_str = self.event_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


@dataclass
class WindowAggregate:
    """1-minute window aggregate emitted to sensors.agg_1m."""
    window_start: str
    window_end: str
    device_id: str
    building: str
    floor: int
    room: str
    reading_count: int
    avg_temperature_c: float
    min_temperature_c: float
    max_temperature_c: float
    avg_power_kw: float
    min_power_kw: float
    max_power_kw: float
    avg_humidity_pct: float
    min_humidity_pct: float
    max_humidity_pct: float
    avg_occupancy: float
    emitted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class AlertMessage:
    """Anomaly alert event emitted to sensors.alerts."""
    alert_id: str
    device_id: str
    building: str
    floor: int
    room: str
    event_ts: str
    anomaly_type: AnomalyType
    severity: str  # "INFO", "WARNING", "CRITICAL"
    description: str
    metric_name: str
    metric_value: Optional[float]
    threshold_value: Optional[float]
    emitted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        res = asdict(self)
        res["anomaly_type"] = self.anomaly_type.value
        return res

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class DeadLetterRecord:
    """Dead letter queue record emitted to sensors.dead_letter."""
    raw_payload: str
    error_reason: str
    error_category: str  # "JSON_PARSE_ERROR", "SCHEMA_ERROR", "RANGE_ERROR", "DUPLICATE_ERROR", "LATE_EVENT"
    rejected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    device_id: Optional[str] = None
    ingest_seq: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class ValidationResult:
    """Outcome of validating a raw payload."""
    is_valid: bool
    reading: Optional[SensorReading] = None
    error_reason: Optional[str] = None
    error_category: Optional[str] = None


# Range constraints for physical sensors
TEMP_MIN_C = -20.0
TEMP_MAX_C = 60.0
HUMIDITY_MIN = 0.0
HUMIDITY_MAX = 100.0
POWER_MIN_KW = 0.0
POWER_MAX_KW = 50.0
OCCUPANCY_MIN = 0
OCCUPANCY_MAX = 500


def validate_reading(data: Dict[str, Any]) -> ValidationResult:
    """
    Validate raw dictionary against schema and physical domain ranges.
    """
    required_fields = [
        "device_id", "building", "floor", "room",
        "event_ts", "ingest_seq", "temperature_c",
        "humidity_pct", "power_kw", "occupancy"
    ]

    for req in required_fields:
        if req not in data:
            return ValidationResult(
                is_valid=False,
                error_reason=f"Missing required field: '{req}'",
                error_category="SCHEMA_ERROR"
            )
        if data[req] is None:
            return ValidationResult(
                is_valid=False,
                error_reason=f"Field '{req}' cannot be null",
                error_category="SCHEMA_ERROR"
            )

    try:
        reading = SensorReading.from_dict(data)
    except (ValueError, TypeError) as e:
        return ValidationResult(
            is_valid=False,
            error_reason=f"Type cast error: {str(e)}",
            error_category="SCHEMA_ERROR"
        )

    # Validate ISO timestamp
    try:
        _ = reading.parsed_event_ts
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            error_reason=f"Invalid event_ts format: {str(e)}",
            error_category="SCHEMA_ERROR"
        )

    # Validate sequence
    if reading.ingest_seq < 0:
        return ValidationResult(
            is_valid=False,
            error_reason=f"ingest_seq must be non-negative, got {reading.ingest_seq}",
            error_category="RANGE_ERROR"
        )

    # Physical range checks
    if not (TEMP_MIN_C <= reading.temperature_c <= TEMP_MAX_C):
        return ValidationResult(
            is_valid=False,
            error_reason=f"Temperature {reading.temperature_c}°C outside range [{TEMP_MIN_C}, {TEMP_MAX_C}]",
            error_category="RANGE_ERROR"
        )

    if not (HUMIDITY_MIN <= reading.humidity_pct <= HUMIDITY_MAX):
        return ValidationResult(
            is_valid=False,
            error_reason=f"Humidity {reading.humidity_pct}% outside range [{HUMIDITY_MIN}, {HUMIDITY_MAX}]",
            error_category="RANGE_ERROR"
        )

    if not (POWER_MIN_KW <= reading.power_kw <= POWER_MAX_KW):
        return ValidationResult(
            is_valid=False,
            error_reason=f"Power {reading.power_kw}kW outside range [{POWER_MIN_KW}, {POWER_MAX_KW}]",
            error_category="RANGE_ERROR"
        )

    if not (OCCUPANCY_MIN <= reading.occupancy <= OCCUPANCY_MAX):
        return ValidationResult(
            is_valid=False,
            error_reason=f"Occupancy {reading.occupancy} outside range [{OCCUPANCY_MIN}, {OCCUPANCY_MAX}]",
            error_category="RANGE_ERROR"
        )

    return ValidationResult(is_valid=True, reading=reading)
