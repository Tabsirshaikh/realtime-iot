"""
End-to-End Pipeline Tests for P2 (Processor), P3 (Alerts & DuckDB), and P4 (Polars).
"""

import json
import os
import tempfile
import pytest

from processor import (
    validate_raw_message,
    AnomalyEngine,
    TumblingWindowAggregator,
    StreamProcessor,
)
from make_history import generate_synthetic_history
from analytics import (
    duckdb_query_1_room_hourly_temp,
    duckdb_query_2_room_daily_power,
    duckdb_query_3_top10_anomalies,
    duckdb_query_4_readings_per_device,
    polars_query_1_room_hourly_temp,
    polars_query_2_room_daily_power,
    polars_query_3_top10_anomalies,
    polars_query_4_readings_per_device,
)


def test_p2_validation_success():
    raw = json.dumps({
        "device_id": "sensor-01",
        "room": "Room-201",
        "event_ts": "2026-10-08T12:00:00.000Z",
        "temperature_c": 22.5,
        "power_kw": 1.5,
    })
    reading, err = validate_raw_message(raw)
    assert err is None
    assert reading["device_id"] == "sensor-01"
    assert reading["temperature_c"] == 22.5


def test_p2_validation_failures():
    # 1. Malformed JSON
    _, err = validate_raw_message("{invalid_json: 123")
    assert "Malformed JSON" in err

    # 2. Missing field
    _, err = validate_raw_message(json.dumps({"device_id": "sensor-01", "room": "Room-201"}))
    assert "Missing required field" in err

    # 3. Null field
    _, err = validate_raw_message(json.dumps({
        "device_id": "sensor-01",
        "room": "Room-201",
        "event_ts": "2026-10-08T12:00:00.000Z",
        "temperature_c": None,
        "power_kw": 1.5,
    }))
    assert "cannot be null" in err

    # 4. Negative power
    _, err = validate_raw_message(json.dumps({
        "device_id": "sensor-01",
        "room": "Room-201",
        "event_ts": "2026-10-08T12:00:00.000Z",
        "temperature_c": 22.0,
        "power_kw": -5.0,
    }))
    assert "cannot be negative" in err


def test_p2_tumbling_window_aggregator():
    aggregator = TumblingWindowAggregator(window_sec=60)
    
    # Add 3 readings for the same window
    for i in range(3):
        aggregator.add_reading({
            "device_id": "sensor-01",
            "room": "Room-201",
            "event_ts": f"2026-10-08T12:00:{i*10:02d}.000Z",
            "temperature_c": 20.0 + i,
            "power_kw": 1.0 + i,
        })

    closed = aggregator.flush_all()
    assert len(closed) == 1
    w = closed[0]
    assert w["device_id"] == "sensor-01"
    assert w["n"] == 3
    assert w["avg_temp"] == 21.0
    assert w["max_temp"] == 22.0
    assert w["avg_power"] == 2.0


def test_p3_anomaly_hard_limits():
    engine = AnomalyEngine(temp_threshold_c=45.0, power_threshold_kw=10.0)

    # 1. Normal reading
    alerts = engine.check_anomalies({
        "device_id": "sensor-01",
        "event_ts": "2026-10-08T12:00:00.000Z",
        "temperature_c": 22.0,
        "power_kw": 1.5,
    })
    assert len(alerts) == 0

    # 2. Temp Spike > 45 C
    alerts = engine.check_anomalies({
        "device_id": "sensor-01",
        "event_ts": "2026-10-08T12:00:01.000Z",
        "temperature_c": 52.0,
        "power_kw": 1.5,
    })
    assert len(alerts) == 1
    assert alerts[0]["type"] == "temp_spike_hard_limit"

    # 3. Power Surge > 10 kW
    alerts = engine.check_anomalies({
        "device_id": "sensor-01",
        "event_ts": "2026-10-08T12:00:02.000Z",
        "temperature_c": 22.0,
        "power_kw": 15.0,
    })
    assert any(a["type"] == "power_surge_hard_limit" for a in alerts)


def test_p3_p4_duckdb_vs_polars_parity(tmp_path):
    parquet_file = str(tmp_path / "temp_history.parquet")
    generate_synthetic_history(output_path=parquet_file, total_rows=5_000, device_count=10)

    # Q1 Parity
    d_q1 = duckdb_query_1_room_hourly_temp(parquet_file)
    p_q1 = polars_query_1_room_hourly_temp(parquet_file)
    assert d_q1.shape[0] == p_q1.shape[0]

    # Q2 Parity
    d_q2 = duckdb_query_2_room_daily_power(parquet_file)
    p_q2 = polars_query_2_room_daily_power(parquet_file)
    assert d_q2.shape[0] == p_q2.shape[0]

    # Q3 Parity
    d_q3 = duckdb_query_3_top10_anomalies(parquet_file)
    p_q3 = polars_query_3_top10_anomalies(parquet_file)
    assert d_q3.shape[0] == p_q3.shape[0]

    # Q4 Parity
    d_q4 = duckdb_query_4_readings_per_device(parquet_file)
    p_q4 = polars_query_4_readings_per_device(parquet_file)
    assert d_q4.shape[0] == p_q4.shape[0]
