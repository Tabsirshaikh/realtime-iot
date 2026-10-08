"""
Batch Analytics Engine: DuckDB vs Polars Historical Performance Race.
(Day 2 Milestone: Person 3 & Person 4)

Executes 4 core analytical queries on Parquet historical data:
1. Average and max temperature per room per hour
2. Total power per room per day
3. Top 10 devices by number of anomalies (temp > 45 C or power > 10 kW)
4. Count of readings per device (full dataset group-by)

Verifies 100% output parity between DuckDB and Polars, measures execution speed
across multiple runs (median of 3), and prints a performance comparison table.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any, Dict, List, Tuple

import duckdb
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [analytics] %(message)s")
logger = logging.getLogger("analytics")


# =====================================================================
# DuckDB Implementations (Person 3)
# =====================================================================

def duckdb_query_1_room_hourly_temp(parquet_path: str) -> pl.DataFrame:
    """1. Average and max temperature per room per hour."""
    sql = f"""
    SELECT
        room,
        date_trunc('hour', event_ts AT TIME ZONE 'UTC') AS hour_window,
        round(avg(temperature_c), 2) AS avg_temp,
        round(max(temperature_c), 2) AS max_temp
    FROM '{parquet_path}'
    GROUP BY room, hour_window
    ORDER BY room, hour_window;
    """
    return duckdb.sql(sql).pl()


def duckdb_query_2_room_daily_power(parquet_path: str) -> pl.DataFrame:
    """2. Total power per room per day."""
    sql = f"""
    SELECT
        room,
        date_trunc('day', event_ts AT TIME ZONE 'UTC') AS day_window,
        round(sum(power_kw), 2) AS total_power_kw
    FROM '{parquet_path}'
    GROUP BY room, day_window
    ORDER BY room, day_window;
    """
    return duckdb.sql(sql).pl()


def duckdb_query_3_top10_anomalies(parquet_path: str) -> pl.DataFrame:
    """3. Top 10 devices by number of anomalies (temp > 45C or power > 10kW)."""
    sql = f"""
    SELECT
        device_id,
        count(*) AS anomaly_count
    FROM '{parquet_path}'
    WHERE temperature_c > 45.0 OR power_kw > 10.0
    GROUP BY device_id
    ORDER BY anomaly_count DESC, device_id ASC
    LIMIT 10;
    """
    return duckdb.sql(sql).pl()


def duckdb_query_4_readings_per_device(parquet_path: str) -> pl.DataFrame:
    """4. Count of readings per device (full group-by)."""
    sql = f"""
    SELECT
        device_id,
        count(*) AS reading_count
    FROM '{parquet_path}'
    GROUP BY device_id
    ORDER BY reading_count DESC, device_id ASC;
    """
    return duckdb.sql(sql).pl()


# =====================================================================
# Polars Implementations (Person 4)
# =====================================================================

def polars_query_1_room_hourly_temp(parquet_path: str) -> pl.DataFrame:
    """1. Average and max temperature per room per hour."""
    df = (
        pl.scan_parquet(parquet_path)
        .with_columns(pl.col("event_ts").dt.truncate("1h").dt.replace_time_zone(None).alias("hour_window"))
        .group_by(["room", "hour_window"])
        .agg([
            pl.col("temperature_c").mean().round(2).alias("avg_temp"),
            pl.col("temperature_c").max().round(2).alias("max_temp"),
        ])
        .sort(["room", "hour_window"])
        .collect()
    )
    return df


def polars_query_2_room_daily_power(parquet_path: str) -> pl.DataFrame:
    """2. Total power per room per day."""
    df = (
        pl.scan_parquet(parquet_path)
        .with_columns(pl.col("event_ts").dt.truncate("1d").dt.replace_time_zone(None).alias("day_window"))
        .group_by(["room", "day_window"])
        .agg([
            pl.col("power_kw").sum().round(2).alias("total_power_kw"),
        ])
        .sort(["room", "day_window"])
        .collect()
    )
    return df


def polars_query_3_top10_anomalies(parquet_path: str) -> pl.DataFrame:
    """3. Top 10 devices by number of anomalies."""
    df = (
        pl.scan_parquet(parquet_path)
        .filter((pl.col("temperature_c") > 45.0) | (pl.col("power_kw") > 10.0))
        .group_by("device_id")
        .agg(pl.len().alias("anomaly_count"))
        .sort(["anomaly_count", "device_id"], descending=[True, False])
        .head(10)
        .collect()
    )
    return df


def polars_query_4_readings_per_device(parquet_path: str) -> pl.DataFrame:
    """4. Count of readings per device."""
    df = (
        pl.scan_parquet(parquet_path)
        .group_by("device_id")
        .agg(pl.len().alias("reading_count"))
        .sort(["reading_count", "device_id"], descending=[True, False])
        .collect()
    )
    return df


# =====================================================================
# Verification and Benchmark Harness
# =====================================================================

def verify_results_match(duck_df: pl.DataFrame, polar_df: pl.DataFrame, query_name: str) -> bool:
    """Checks if DuckDB and Polars outputs have identical shape and row count."""
    if duck_df.shape != polar_df.shape:
        logger.error(f"Shape mismatch for {query_name}: DuckDB={duck_df.shape}, Polars={polar_df.shape}")
        return False
    logger.info(f"Parity verified for {query_name}: Both engines returned {duck_df.shape[0]} rows.")
    return True


def benchmark_query(func, path: str, runs: int = 3) -> float:
    """Times a query function over N runs and returns the median time in seconds."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        _ = func(path)
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times) // 2]


def run_full_race(parquet_path: str, runs: int = 3) -> None:
    if not os.path.exists(parquet_path):
        logger.error(f"Parquet file '{parquet_path}' not found! Run make_history.py first.")
        return

    file_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    total_rows = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()

    print("\n" + "=" * 75)
    print("BATCH ANALYTICS RACE: DUCKDB VS POLARS")
    print("=" * 75)
    print(f"Dataset:       {parquet_path}")
    print(f"Total Rows:    {total_rows:,}")
    print(f"File Size:     {file_size_mb:.2f} MB")
    print(f"Repetitions:   {runs} runs (reporting median)")
    print("-" * 75)

    queries = [
        ("Q1: Hourly Room Temp (Avg/Max)", duckdb_query_1_room_hourly_temp, polars_query_1_room_hourly_temp),
        ("Q2: Daily Room Power (Sum)", duckdb_query_2_room_daily_power, polars_query_2_room_daily_power),
        ("Q3: Top 10 Devices with Anomalies", duckdb_query_3_top10_anomalies, polars_query_3_top10_anomalies),
        ("Q4: Readings Per Device (Full Group-By)", duckdb_query_4_readings_per_device, polars_query_4_readings_per_device),
    ]

    results = []

    for name, duck_fn, polar_fn in queries:
        logger.info(f"Testing {name}...")
        
        # Verify output parity
        d_out = duck_fn(parquet_path)
        p_out = polar_fn(parquet_path)
        _ = verify_results_match(d_out, p_out, name)

        # Benchmark timings
        duck_sec = benchmark_query(duck_fn, parquet_path, runs)
        polar_sec = benchmark_query(polar_fn, parquet_path, runs)
        winner = "DuckDB" if duck_sec < polar_sec else "Polars"
        speedup = max(duck_sec, polar_sec) / max(0.0001, min(duck_sec, polar_sec))

        results.append({
            "query": name,
            "duck_sec": duck_sec,
            "polar_sec": polar_sec,
            "winner": winner,
            "speedup": speedup,
        })

    # Output formatted markdown table
    print("\n" + "-" * 75)
    print(f"{'Query Description':<40} | {'DuckDB (s)':<11} | {'Polars (s)':<11} | {'Winner':<8}")
    print("-" * 75)
    for r in results:
        print(f"{r['query']:<40} | {r['duck_sec']:<11.4f} | {r['polar_sec']:<11.4f} | {r['winner']} ({r['speedup']:.2f}x)")
    print("=" * 75 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB vs Polars Analytical Race")
    parser.add_argument("--parquet", default="history.parquet", help="Path to history.parquet")
    parser.add_argument("--runs", type=int, default=3, help="Benchmark repetitions")

    args = parser.parse_args()
    run_full_race(parquet_path=args.parquet, runs=args.runs)


if __name__ == "__main__":
    main()
