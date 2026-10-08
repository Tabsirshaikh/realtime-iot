# Real-Time IoT Stream Processing for Smart Buildings

[![CI Lint & Tests](https://github.com/Tabsirshaikh/realtime-iot/actions/workflows/ci.yml/badge.svg)](https://github.com/Tabsirshaikh/realtime-iot/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Redpanda](https://img.shields.io/badge/broker-Redpanda-red.svg)](https://redpanda.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Conveyor Belt Analogy:**
> Sensors drop continuous readings onto a high-speed conveyor belt (**Redpanda message broker**). Stations along the belt sort, clean, validate, and package items into 1-minute summaries (**Stream Processor with hand-written event windows**). Rejected defective items are tagged and routed to an inspection bin (**Dead Letter topic**). Finished goods flow to a front shelf for instant retrieval (**PostgreSQL hot store & Grafana live dashboard**) and into a central warehouse for long-term audit and analysis (**Delta Lake cold store & DuckDB/Polars batch analytics**). An automated manager runs nightly maintenance and data checks (**Apache Airflow**).

---

## 1. High-Level Architecture

```
+------------------+         +-------------------------------------------------------+
| Sensor Simulator | ------> | Redpanda Broker (sensors.raw)                        |
| (Fault Injector) |         +-------------------------------------------------------+
+------------------+                                     |
                                                         v
                                       +-----------------------------------+
                                       | Python Stream Processor           |
                                       | - Schema & Range Validation       |
                                       | - Monotonic Deduplication         |
                                       | - Watermarked Event-Time Windows  |
                                       | - Anomaly Detection Engine        |
                                       +-----------------------------------+
                                            |           |          |
         +----------------------------------+           |          +----------------------------------+
         |                                              |                                             |
         v                                              v                                             v
+------------------------+             +------------------------+                     +------------------------+
| sensors.clean          |             | sensors.agg_1m         |                     | sensors.alerts         |
| sensors.dead_letter    |             | (1-minute aggregates)  |                     | (anomalies detected)   |
+------------------------+             +------------------------+                     +------------------------+
         |                                              |                                             |
         +----------------------------------+-----------+---------------------------------------------+
                                            |
                                            v
                      +---------------------+---------------------+
                      |                                           |
                      v                                           v
       +-------------------------------+         +----------------------------------+
       | Hot Store: PostgreSQL         |         | Cold Store: Delta Lake (Parquet) |
       | - readings_clean              |         | - Partitioned by date/device     |
       | - agg_1m                      |         | - Micro-batch appends            |
       | - alerts                      |         | - Idempotent upserts             |
       +-------------------------------+         +----------------------------------+
                      |                                           |
                      v                                           v
       +-------------------------------+         +----------------------------------+
       | Control Room: Grafana         |         | Batch Analytics & Rollups        |
       | - Real-time metrics per room  |         | - DuckDB vs Polars comparison    |
       | - Live anomaly alerts panel   |         | - Airflow DAGs (compaction/QC)   |
       | - Dead-letter failure monitor |         +----------------------------------+
       +-------------------------------+
```

---

## 2. Day-1 Contract (Frozen Specification)

All components across both Person 1 (Stream Side) and Person 2 (Storage & Serving Side) strictly adhere to this frozen contract defined in `shared/schema.py`.

### 2.1 Message Schema (`shared/schema.py`)

Each simulated smart building sensor reading is published as a JSON payload:

| Field | Type | Description | Constraints / Examples |
|---|---|---|---|
| `device_id` | `string` | Unique sensor identifier | `sensor-b1-f02-r204-temp` |
| `building` | `string` | Building name or identifier | `HQ-West`, `Tower-A` |
| `floor` | `integer` | Building floor number | `>= 0` (e.g. `2`) |
| `room` | `string` | Room or zone identifier | `Room-204`, `Lobby-North` |
| `event_ts` | `string` | ISO 8601 UTC timestamp | `2026-10-08T12:00:00.000Z` |
| `ingest_seq`| `integer` | Monotonic per-device sequence | `>= 0`, used for deduplication |
| `temperature_c` | `float` | Ambient temperature (°C) | Normal: 18–26°C; Valid: -20 to 60°C |
| `humidity_pct` | `float` | Relative humidity (%) | Normal: 30–65%; Valid: 0 to 100% |
| `power_kw` | `float` | Instantaneous power draw (kW) | Normal: 0.2–5.0 kW; Valid: 0 to 50 kW |
| `occupancy` | `integer` | Headcount / people count | Normal: 0–30; Valid: 0 to 500 |

### 2.2 Kafka / Redpanda Topics

| Topic Name | Purpose | Message Type |
|---|---|---|
| `sensors.raw` | Untouched, raw simulator output (including faulty payloads) | `SensorReading` (raw JSON) |
| `sensors.clean` | Validated, deduplicated sensor events | `SensorReading` |
| `sensors.dead_letter`| Rejected malformed/out-of-bounds events + error diagnostic | `DeadLetterRecord` |
| `sensors.agg_1m` | Tumbling 1-minute window rollups (avg, min, max, count) | `WindowAggregate` |
| `sensors.alerts` | Real-time detected anomalies and alert events | `AlertMessage` |

### 2.3 Monitored Anomaly Types

1. **`temperature_spike`**: Abrupt temperature shift exceeding physical limits or dynamic z-score threshold.
2. **`power_surge`**: Sudden spike in instantaneous power draw exceeding safe electrical thresholds.
3. **`sensor_stuck`**: Sensor emitting the identical floating-point value $N$ consecutive times (indicates frozen hardware ADC).
4. **`device_silent`**: Sensor has stopped reporting past the watermark latency threshold ($N$ seconds).

---

## 3. Team Responsibilities & Ownership

| Area | Person 1 (P1 - Stream Side) | Person 2 (P2 - Storage & Serving Side) |
|---|---|---|
| **Infra** | Redpanda broker + Redpanda Console in Docker Compose | PostgreSQL, Grafana, and Airflow in Docker Compose |
| **Data Generation** | Realistic multi-device simulator with fault injection | Database schemas (DDL), Delta Lake table partition design |
| **Stream Logic** | Schema & range validation, deduplication, tumbling & sliding window aggregations, watermark engine, anomaly rules | Consumer sinks for Postgres and Delta Lake, idempotent batch upserts |
| **Batch & Analytics** | N/A | DuckDB vs. Polars historical query benchmarks |
| **Orchestration** | N/A | Airflow DAGs: nightly rollups, Delta compaction, vacuum |
| **Visualization** | N/A | Grafana dashboard (live metrics, alerts, health) |
| **Testing & Benchmark**| End-to-end latency (p50/p95), throughput at 10/100/1000 devices, broker failure/recovery tests | Data quality assertions, query performance benchmarks |

---

## 4. Repository Structure

```
realtime-iot/
├── .github/
│   └── workflows/
│       └── ci.yml             # GitHub Actions CI for linting and test runs
├── docker-compose.yml         # Unified multi-service deployment
├── shared/                    # Day-1 Shared Contract (Frozen)
│   ├── __init__.py
│   └── schema.py              # Single source of truth models & validation
├── simulator/                 # [Person 1 / Stream Side]
│   ├── __init__.py
│   ├── device.py              # Sensor physics & drift simulation
│   ├── faults.py              # Injection engine (spikes, stuck, nulls, duplicates)
│   ├── publisher.py           # Confluent-Kafka / Redpanda producer client
│   ├── main.py                # Simulator CLI entrypoint
│   └── Dockerfile
├── processor/                 # [Person 1 / Stream Side]
│   ├── __init__.py
│   ├── validator.py           # Schema, range & null validation
│   ├── dedup.py               # Monotonic sequence deduplication cache
│   ├── windows.py             # Pure-Python event-time tumbling & sliding windows
│   ├── watermark.py           # Allowed lateness & watermark tracker
│   ├── anomalies.py           # Rule-based & statistical anomaly detection
│   ├── main.py                # Stream processor consumer & router loop
│   └── Dockerfile
├── sinks/                     # [Person 2 / Storage & Serving Side]
│   ├── postgres_sink.py       # Batch consumer writing to PostgreSQL
│   └── delta_sink.py          # Micro-batch consumer appending to Delta Lake
├── analytics/                 # [Person 2 / Storage & Serving Side]
│   ├── duckdb_queries.py      # Hourly/daily rollups on Delta Lake
│   └── polars_queries.py      # Side-by-side performance benchmarks
├── airflow/                   # [Person 2 / Storage & Serving Side]
│   └── dags/
│       ├── nightly_rollup.py
│       ├── delta_compaction.py
│       └── data_quality.py
├── grafana/                   # [Person 2 / Storage & Serving Side]
│   ├── provisioning/
│   └── dashboards/
├── benchmarks/                # [Person 1 / Stream Side]
│   └── run_benchmark.py       # 10 / 100 / 1000 device throughput & latency harness
├── tests/
│   ├── test_schema.py         # Contract validation tests
│   ├── test_simulator.py      # Device simulation & fault injection tests
│   ├── test_processor.py      # Validation, deduplication, windowing tests
│   └── test_anomalies.py      # Anomaly rule assertion tests
├── requirements.txt
└── README.md
```

---

## 5. Quickstart

### Prerequisites
- Python 3.10+
- Docker and Docker Compose (v2+)

### 1. Clone & Set Up Python Environment
```bash
git clone https://github.com/Tabsirshaikh/realtime-iot.git
cd realtime-iot

python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Start Services via Docker Compose
```bash
docker compose up -d redpanda redpanda-console postgres grafana
```
- **Redpanda Console UI**: `http://localhost:8080`
- **Grafana Dashboard**: `http://localhost:3000` (admin / admin)
- **PostgreSQL**: `localhost:5432` (`iot_user` / `iot_password` / `iot_db`)

### 3. Run the Stream Processor (P1)
```bash
python -m processor.main --bootstrap-servers localhost:9092
```

### 4. Run the Sensor Simulator (P1)
```bash
# Run simulator with 20 devices, publishing every 1 second, 5% fault probability
python -m simulator.main --devices 20 --rate-sec 1.0 --fault-rate 0.05
```

### 5. Run the Test Suite
```bash
pytest tests/ -v
```

---

## 6. Git Workflow

- **`main`**: Protected branch containing stable releases, documentation, and the shared contract.
- **`p1-stream-pipeline`**: Person 1's active feature branch (Simulator, Redpanda infra, Stream Processor, Benchmarks).
- **`p2-storage-serving`**: Person 2's active feature branch (Postgres/Delta sinks, Analytics, Airflow, Grafana).
- Merge to `main` via reviewed Pull Requests with passing CI checks.
