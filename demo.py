"""
End-to-End Live Demonstration Runner for IoT Stream Processing Pipeline.

Runs all 4 team roles in a single live demonstration flow:
1. Simulates telemetry with normal readings, spikes, and corrupted rows (P1).
2. Runs processor: cleans readings, rejects defective rows, computes 1-minute rollups (P2).
3. Evaluates real-time anomaly rules (temp > 45 C, power > 10 kW, 3-sigma) (P3).
4. Generates Parquet historical data and runs the DuckDB vs Polars performance race (P3 + P4).
5. Executes the crash & recovery audit (P1).
"""

import json
import os
import time
from processor import StreamProcessor
from simulator import DeviceState, SimulatorEngine
from make_history import generate_synthetic_history
from analytics import run_full_race
from failure_test import run_mock_failure_recovery_test


def run_full_demo():
    print("\n" + "=" * 70)
    print("STEP 1: STREAM PROCESSING & ANOMALY DETECTION (P1 + P2 + P3)")
    print("=" * 70)

    # Initialize processor in mock mode
    proc = StreamProcessor(mock_mode=True)

    # Initialize simulator emitting clean readings, spikes, and bad rows
    sim = SimulatorEngine(
        device_count=5,
        inject_spikes=True,
        inject_bad_rows=True,
        mock_mode=True,
    )

    print("Emitting 60 simulated telemetry readings through processor...")
    for i in range(60):
        dev = sim.devices[i % len(sim.devices)]
        payload, note = sim.generate_payload(dev)
        raw_str = payload if isinstance(payload, str) else json.dumps(payload)
        proc.process_message_payload(raw_str)

    # Flush 1-minute tumbling window aggregations
    proc.flush_windows()

    print("\n" + "-" * 70)
    print("STREAM PROCESSOR METRICS SUMMARY:")
    print("-" * 70)
    print(f"Total Received:         {proc.stats['received']}")
    print(f"Readings Saved:         {proc.stats['readings_written']}  (clean stream -> readings table)")
    print(f"Rejected Rows:          {proc.stats['rejected_written']}   (defective items -> rejected table)")
    print(f"Alerts Triggered:       {proc.stats['alerts_written']}   (fires/surges -> alerts table)")
    print(f"1-min Rollups Saved:    {proc.stats['aggregates_written']}   (tumbling window -> agg_1m table)")

    if proc.db.mock_db["alerts"]:
        print("\nSample Detected Alerts:")
        for a in proc.db.mock_db["alerts"][:3]:
            print(f"  [ALERT] Device: {a['device_id']} | Type: {a['type']} | Value: {a['value']}")

    if proc.db.mock_db["rejected"]:
        print("\nSample Rejected Bad Rows:")
        for r in proc.db.mock_db["rejected"][:3]:
            print(f"  [REJECTED] Reason: {r['reason']}")

    if proc.db.mock_db["agg_1m"]:
        print("\nSample 1-Minute Window Rollups:")
        for w in proc.db.mock_db["agg_1m"][:3]:
            print(f"  [AGG_1M] Device: {w['device_id']} | Avg Temp: {w['avg_temp']} C | Max Temp: {w['max_temp']} C | n={w['n']}")

    print("\n" + "=" * 70)
    print("STEP 2: BATCH ANALYTICS RACE: DUCKDB VS POLARS (P3 + P4)")
    print("=" * 70)
    parquet_path = "history.parquet"
    generate_synthetic_history(output_path=parquet_path, total_rows=100_000, device_count=20)
    run_full_race(parquet_path=parquet_path, runs=3)

    if os.path.exists(parquet_path):
        os.remove(parquet_path)

    print("\n" + "=" * 70)
    print("STEP 3: BROKER CRASH & RECOVERY AUDIT (P1)")
    print("=" * 70)
    success = run_mock_failure_recovery_test()
    assert success, "Crash recovery audit failed"

    print("\n" + "=" * 70)
    print("ALL STAGES RAN SUCCESSFULLY - PIPELINE VERIFIED!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_full_demo()
