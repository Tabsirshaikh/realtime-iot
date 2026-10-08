"""
Historical Parquet Dataset Generator & Exporter (Day 2: Person 3).

Generates synthetic smart building readings for batch analytics benchmarking
(DuckDB vs Polars race on historical data).

Parquet schema (same as PostgreSQL readings table):
- device_id: string
- room: string
- event_ts: timestamp[us, tz=UTC]
- temperature_c: double
- power_kw: double
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [make_history] %(message)s")
logger = logging.getLogger("make_history")


def generate_synthetic_history(
    output_path: str = "history.parquet",
    total_rows: int = 1_000_000,
    device_count: int = 50,
    batch_size: int = 100_000,
) -> None:
    """
    Generates total_rows synthetic readings written directly to Parquet using PyArrow batches.
    """
    logger.info(f"Generating {total_rows:,} historical readings into '{output_path}'...")
    start_time = time.perf_counter()

    schema = pa.schema([
        ("device_id", pa.string()),
        ("room", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("temperature_c", pa.float64()),
        ("power_kw", pa.float64()),
    ])

    devices = [f"sensor-{i:03d}" for i in range(1, device_count + 1)]
    rooms = [f"Room-{200 + (i % 8)}" for i in range(1, device_count + 1)]

    # 30 days of historical data span
    base_time = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    seconds_span = 30 * 86400

    writer = pq.ParquetWriter(output_path, schema, compression="snappy")
    rows_written = 0

    try:
        while rows_written < total_rows:
            current_batch_size = min(batch_size, total_rows - rows_written)

            dev_col = []
            room_col = []
            ts_col = []
            temp_col = []
            power_col = []

            for _ in range(current_batch_size):
                idx = random.randint(0, device_count - 1)
                dev = devices[idx]
                rm = rooms[idx]

                # Random timestamp within 30-day window
                offset_sec = random.randint(0, seconds_span)
                event_dt = base_time + timedelta(seconds=offset_sec)

                # Realistic temperatures with occasional anomalies
                is_anomaly = random.random() < 0.01
                if is_anomaly:
                    temp = round(random.uniform(46.0, 60.0), 2)
                    power = round(random.uniform(11.0, 25.0), 3)
                else:
                    # Diurnal curve based on hour of day
                    hour = event_dt.hour
                    diurnal = math.sin((hour - 6) / 24 * 2 * math.pi) * 2.5
                    temp = round(21.5 + diurnal + random.gauss(0, 0.5), 2)
                    power = round(1.2 + abs(diurnal) * 0.4 + random.gauss(0, 0.2), 3)
                    temp = max(16.0, min(30.0, temp))
                    power = max(0.1, power)

                dev_col.append(dev)
                room_col.append(rm)
                ts_col.append(event_dt)
                temp_col.append(temp)
                power_col.append(power)

            batch = pa.RecordBatch.from_arrays(
                [
                    pa.array(dev_col, type=pa.string()),
                    pa.array(room_col, type=pa.string()),
                    pa.array(ts_col, type=pa.timestamp("us", tz="UTC")),
                    pa.array(temp_col, type=pa.float64()),
                    pa.array(power_col, type=pa.float64()),
                ],
                schema=schema,
            )
            writer.write_batch(batch)
            rows_written += current_batch_size

            if rows_written % 500_000 == 0 or rows_written == total_rows:
                logger.info(f"Progress: {rows_written:,} / {total_rows:,} rows written...")

    finally:
        writer.close()

    elapsed = time.perf_counter() - start_time
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"Dataset generated in {elapsed:.2f}s ({total_rows / elapsed:,.0f} rows/s)")
    logger.info(f"File size: {file_size_mb:.2f} MB on disk")


def export_from_postgres(
    output_path: str = "history_live.parquet",
    host: str = "localhost",
    port: int = 5432,
    dbname: str = "iot_db",
    user: str = "iot_user",
    password: str = "iot_password",
) -> None:
    """Exports live PostgreSQL 'readings' table into Parquet."""
    logger.info(f"Connecting to PostgreSQL to export 'readings' -> {output_path}...")
    import psycopg2

    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()
    cur.execute("SELECT device_id, room, event_ts, temperature_c, power_kw FROM readings;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        logger.warning("No rows found in PostgreSQL 'readings' table.")
        return

    schema = pa.schema([
        ("device_id", pa.string()),
        ("room", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("temperature_c", pa.float64()),
        ("power_kw", pa.float64()),
    ])

    devs, rooms, tss, temps, powers = zip(*rows)
    table = pa.Table.from_arrays(
        [
            pa.array(devs, type=pa.string()),
            pa.array(rooms, type=pa.string()),
            pa.array(tss, type=pa.timestamp("us", tz="UTC")),
            pa.array(temps, type=pa.float64()),
            pa.array(powers, type=pa.float64()),
        ],
        schema=schema,
    )
    pq.write_table(table, output_path, compression="snappy")
    logger.info(f"Exported {len(rows):,} rows from PostgreSQL to '{output_path}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Parquet history dataset")
    parser.add_argument("--output", default="history.parquet", help="Output Parquet path")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Total rows to generate")
    parser.add_argument("--devices", type=int, default=50, help="Device count")
    parser.add_argument("--export-postgres", action="store_true", help="Export live PostgreSQL data instead")

    args = parser.parse_args()

    if args.export_postgres:
        export_from_postgres(output_path=args.output)
    else:
        generate_synthetic_history(
            output_path=args.output,
            total_rows=args.rows,
            device_count=args.devices,
        )


if __name__ == "__main__":
    main()
