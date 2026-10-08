"""
Unit tests for Person 1 (P1) Stream Infra, Simulator, and Contract.
"""

import json
import pytest
from simulator import DeviceState, SimulatorEngine


def test_device_state_realistic_drift():
    dev = DeviceState(device_id="sensor-01", room="Room-201")
    reading = dev.next_reading()

    assert reading["device_id"] == "sensor-01"
    assert reading["room"] == "Room-201"
    assert "event_ts" in reading
    assert 18.0 <= reading["temperature_c"] <= 28.0
    assert 0.2 <= reading["power_kw"] <= 4.5


def test_simulator_spike_injection():
    sim = SimulatorEngine(
        device_count=5,
        inject_spikes=True,
        mock_mode=True,
    )
    dev = sim.devices[0]

    # Sample multiple generations to catch spike
    found_spike = False
    for _ in range(50):
        payload, note = sim.generate_payload(dev)
        if "SPIKE" in note:
            found_spike = True
            # Rule 1: Temperature above 45 C or power above 10 kW
            assert payload["temperature_c"] > 45.0 or payload["power_kw"] > 10.0
            break

    assert found_spike, "Expected at least one spike to be injected across 50 iterations"


def test_simulator_bad_rows_injection():
    sim = SimulatorEngine(
        device_count=5,
        inject_bad_rows=True,
        mock_mode=True,
    )
    dev = sim.devices[0]

    found_bad_row = False
    for _ in range(50):
        payload, note = sim.generate_payload(dev)
        if "BAD-ROW" in note:
            found_bad_row = True
            if isinstance(payload, str):
                # Malformed JSON
                assert "BROKEN_SYNTAX" in payload
            else:
                # Missing or null or negative field
                is_invalid = (
                    "temperature_c" not in payload
                    or payload.get("power_kw") is None
                    or payload.get("power_kw", 0) < 0
                )
                assert is_invalid
            break

    assert found_bad_row, "Expected at least one bad row to be injected across 50 iterations"


def test_schema_sql_has_required_tables():
    with open("schema.sql", "r") as f:
        sql = f.read().lower()

    assert "create table if not exists readings" in sql
    assert "create table if not exists agg_1m" in sql
    assert "create table if not exists alerts" in sql
    assert "create table if not exists rejected" in sql
