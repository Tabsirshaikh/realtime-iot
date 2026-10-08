# IoT Stream Pipeline: Two-Day Plan (Grafana + DuckDB + Polars)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Redpanda](https://img.shields.io/badge/broker-Redpanda-red.svg)](https://redpanda.com/)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-blue.svg)](https://www.postgresql.org/)
[![Grafana](https://img.shields.io/badge/dashboard-Grafana-orange.svg)](https://grafana.com/)
[![DuckDB](https://img.shields.io/badge/analytics-DuckDB-yellow.svg)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/analytics-Polars-blueviolet.svg)](https://pola.rs/)

> **Conveyor Belt Analogy:**
> Sensors drop parcels on a conveyor belt (**Redpanda**). An inspector throws out damaged parcels and flags suspicious ones (**Python stream processor**). Good parcels go on a shelf (**Postgres**) that a professional control room screen reads from (**Grafana**). Once a day, a copy of the shelf is packed into flat files (**Parquet**) and two different calculators, **DuckDB** and **Polars**, race to answer the same analytical questions.

---

## 1. System Architecture

```
+-------------------+        +-----------------------------------+
|   simulator.py    | -----> | Redpanda Broker (sensors.raw)     |
| (Spikes/Bad-Rows) |        +-----------------------------------+
+-------------------+                          |
                                               v
                             +-----------------------------------+
                             |           processor.py            |
                             |  - Schema & Type Validation       |
                             |  - Anomaly Rules (3-sigma, caps)  |
                             |  - Tumbling 1-min aggregations    |
                             +-----------------------------------+
                                   |           |          |
                    +--------------+           |          +--------------+
                    |                          |                         |
                    v                          v                         v
          +-------------------+      +-------------------+      +-------------------+
          | readings table    |      | agg_1m table      |      | rejected table    |
          | (clean stream)    |      | (1-min rollups)   |      | (bad rows & logs) |
          +-------------------+      +-------------------+      +-------------------+
                    |                          |                         |
                    +--------------------------+-------------------------+
                                               |
                                               v
                             +-----------------------------------+
                             | PostgreSQL Hot Store              |
                             | + alerts table (sensors.alerts)   |
                             +-----------------------------------+
                                   |                       |
                                   v                       v
                         +-------------------+   +--------------------+
                         | Grafana Dashboard |   | make_history.py    |
                         | - Live room temps |   | (Parquet Export)   |
                         | - Live alerts     |   +--------------------+
                         | - Bad row counter |             |
                         | - Lag & Throughput|             v
                         +-------------------+   +--------------------+
                                                 | analytics.py       |
                                                 | DuckDB vs. Polars  |
                                                 | Performance Race   |
                                                 +--------------------+
```

---

## 2. Technology Stack

| Piece | Tool | Role |
|---|---|---|
| **Broker** | Redpanda | Conveyor belt for incoming streaming events |
| **Processor** | Python + `confluent-kafka` | Validation, anomalies, and windowing inspector |
| **Hot store** | PostgreSQL 16 | Real-time queryable tables for recent data |
| **Dashboard** | Grafana | Live control room monitoring screen (auto-provisioned) |
| **Batch engine 1** | DuckDB | SQL engine on Parquet historical data |
| **Batch engine 2** | Polars | Fast DataFrame engine on Parquet historical data |
| **Runner** | Docker Compose | Starts Redpanda, PostgreSQL, and Grafana in one command |

*Note: Delta Lake and Airflow are streamlined out in favor of direct high-performance Parquet files and scheduled script runs.*

---

## 3. The Contract

### 3.1 Streaming Message Schema (JSON)
Emitted to topic `sensors.raw`:
```json
{
  "device_id": "sensor-101",
  "room": "Room-204",
  "event_ts": "2026-10-08T12:00:00.000Z",
  "temperature_c": 22.45,
  "power_kw": 1.35
}
```

### 3.2 Kafka / Redpanda Topics
- `sensors.raw`: Untouched simulator output (including injected spikes and corrupted payloads).
- `sensors.alerts`: Emitted anomaly alerts when abnormal readings are flagged.

### 3.3 PostgreSQL Relational Tables (`schema.sql`)
1. `readings(device_id TEXT, room TEXT, event_ts TIMESTAMPTZ, temperature_c FLOAT, power_kw FLOAT)`
2. `agg_1m(device_id TEXT, window_start TIMESTAMPTZ, avg_temp FLOAT, max_temp FLOAT, avg_power FLOAT, n INT)`
3. `alerts(device_id TEXT, event_ts TIMESTAMPTZ, type TEXT, value FLOAT)`
4. `rejected(raw_payload TEXT, reason TEXT, ts TIMESTAMPTZ)`

### 3.4 Anomaly Detection Rules
1. **Hard Threshold Rule:** Temperature $> 45^\circ\text{C}$ OR Power draw $> 10\text{ kW}$.
2. **Dynamic 3-Sigma Rule:** Reading more than $3\sigma$ (standard deviations) from the device's rolling history of the last 20 readings.

---

## 4. Repository Layout

```
realtime-iot/
├── docker-compose.yml      # Starts Redpanda, PostgreSQL, and Grafana
├── schema.sql              # PostgreSQL DDL for readings, agg_1m, alerts, rejected
├── simulator.py            # Generates telemetry with --spike and --bad-rows flags
├── processor.py            # Stream consumer: cleans, aggregates, flags anomalies
├── make_history.py         # Generates Parquet historical benchmark datasets
├── analytics.py            # Side-by-side DuckDB vs Polars analytical query race
├── grafana/
│   └── provisioning/
│       ├── datasources/    # Auto-configured PostgreSQL data source
│       └── dashboards/     # Pre-loaded live IoT dashboard JSON
├── requirements.txt
└── README.md
```

---

## 5. Team Work Split

| Member | Area | Scope & Ownership | Done When |
|---|---|---|---|
| **Person 1 (P1)** | **Infra, Simulator, Benchmarks** | `docker-compose.yml`, `schema.sql`, `simulator.py` (with `--spike` and `--bad-rows` injection). Day 2: latency benchmark and failure recovery test. | `docker compose up` starts cleanly and messages flow into `sensors.raw`. |
| **Person 2 (P2)** | **Processor** | `processor.py`: validation, writes clean data to `readings`, rejected rows to `rejected`, and 1-minute tumbling rollups to `agg_1m`. | Rows appear in PostgreSQL within 2 seconds. |
| **Person 3 (P3)** | **Alerts & DuckDB** | Anomaly rules merged into processor (`alerts` table). Day 2: `analytics.py` DuckDB implementation (hourly/daily rollups, top-10 hottest devices), plus `make_history.py`. | Spikes generate alert rows; DuckDB returns correct rollups. |
| **Person 4 (P4)** | **Grafana, Polars, Docs** | Provisioned Grafana dashboard JSON. Day 2: Polars implementation of queries, timing comparison table, README docs, screenshots. | Dashboard updates live; Polars output matches DuckDB output. |

---

## 6. Quickstart Guide

### 1. Start Services via Docker Compose
```bash
docker compose up -d
```
- **Redpanda Console**: [http://localhost:8080](http://localhost:8080)
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (User: `admin` / Pass: `admin`)
- **PostgreSQL**: `localhost:5432` (`iot_user` / `iot_password` / `iot_db`)

### 2. Run the Stream Processor
```bash
python processor.py
```

### 3. Run the Sensor Simulator
```bash
# Normal continuous telemetry (10 devices)
python simulator.py --devices 10 --rate 1.0

# Inject temperature and power spikes
python simulator.py --devices 10 --spike

# Inject malformed and bad rows to test rejection counter
python simulator.py --devices 10 --bad-rows
```

### 4. Run Batch Historical Analytics (DuckDB vs Polars)
```bash
# 1. Generate historical Parquet dataset (e.g. 1M or 5M rows)
python make_history.py --rows 1000000

# 2. Run the analytical query race
python analytics.py --parquet history.parquet
```

---

## 7. Batch Analytics Comparison (Day 2)

Both **DuckDB** and **Polars** run the following 4 queries on `history.parquet`:
1. **Query 1:** Average and maximum temperature per room per hour.
2. **Query 2:** Total power consumption per room per day.
3. **Query 3:** Top 10 devices ranked by number of anomaly events.
4. **Query 4:** Total count of readings grouped by device.

Results are verified for identical parity and timed over multiple runs.
