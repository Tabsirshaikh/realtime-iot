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
| **Runner** | Docker Compose or Kubernetes | Runs the complete streaming stack locally |

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

---

## 8. Real-World Live API Ingestion (Bonus Milestone)

To fulfill the bonus evaluation criteria for real-time external APIs, `live_api_producer.py` streams live meteorological telemetry from global observation stations (New York, London, Tokyo, Mumbai, Sydney) via the Open-Meteo REST API:

```bash
# Ingest live weather data into 'sensors.raw'
python live_api_producer.py --cycles 10 --interval 3.0
```

---

## 9. Apache Spark Analytics & Optimizations (`spark_analytics.py`)

Fulfills requirements for distributed Spark transformations, caching, and partitioning:
- **Transformations:** `.filter()`, `.groupBy()`, `.withColumn()`, `.repartition(4)`
- **Optimizations:** In-memory `.persist(StorageLevel.MEMORY_ONLY)` & shuffle tuning
- **Actions:** `.count()`, `.collect()`, `.show()`

```bash
python spark_analytics.py --rows 100000
```

---

## 10. Automated Benchmark Capture & Visuals

To automatically run the entire pipeline, benchmark latency/throughput, and export presentation-ready `.png` charts into `docs/screenshots/`:

```bash
python capture_results.py
```

Generated charts in `docs/screenshots/`:
- `architecture_diagram.png`: End-to-end pipeline architecture.
- `grafana_dashboard_preview.png`: Control room monitoring screen.
- `latency_distribution.png`: Latency percentiles ($p50, p90, p99$) across scale.
- `throughput_scaling.png`: Events/sec publishing throughput.
- `engine_performance_race.png`: Head-to-head execution times.

---

## 11. Run the Full Stack with Kubernetes

The Kubernetes manifests run Redpanda, PostgreSQL, the stream processor, the simulator, Redpanda Console, and Grafana in the `iot-pipeline` namespace. The simulator runs inside the cluster and sends continuous telemetry to Redpanda, so no host Kafka connection is needed.

### Prerequisites

- A running Kubernetes cluster and a configured `kubectl` context. Docker Desktop Kubernetes, Minikube, and Kind are suitable for local development.
- `kubectl` installed and available in your terminal.
- At least 2 GB of memory available to the cluster. The first deployment downloads container images and Python packages.

Check that `kubectl` is connected to the cluster you intend to use:

```bash
kubectl config current-context
kubectl get nodes
```

### Deploy

Run these commands from the repository root. They create the namespace and database, start Redpanda, and deploy Grafana and the real stream processor:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redpanda.yaml
kubectl apply -f k8s/grafana.yaml
kubectl apply -f k8s/processor.yaml
```

The simulator manifest mounts the repository's `simulator.py` from a ConfigMap. Create or refresh that ConfigMap, then start the simulator:

```bash
kubectl create configmap simulator-source -n iot-pipeline --from-file=simulator.py --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/simulator.yaml
```

For PowerShell, the same commands work as written. Recreate the ConfigMap command after changing `simulator.py` so the pod receives the updated source.

Wait for the workloads to become ready:

```bash
kubectl rollout status deployment/postgres -n iot-pipeline --timeout=180s
kubectl rollout status deployment/redpanda -n iot-pipeline --timeout=180s
kubectl rollout status deployment/grafana -n iot-pipeline --timeout=180s
kubectl rollout status deployment/stream-processor -n iot-pipeline --timeout=240s
kubectl rollout status deployment/iot-simulator -n iot-pipeline --timeout=240s
kubectl rollout status deployment/redpanda-console -n iot-pipeline --timeout=180s
kubectl get pods -n iot-pipeline
```

Redpanda creates the `sensors.raw` and `sensors.alerts` topics. Confirm them with:

```bash
kubectl exec -n iot-pipeline deployment/redpanda -- rpk topic list
```

### Open Grafana and Redpanda Console

Run each port-forward in its own terminal and leave those terminals open:

```bash
kubectl port-forward -n iot-pipeline svc/grafana 3000:3000
```

```bash
kubectl port-forward -n iot-pipeline svc/redpanda-console 8081:8080
```

Open [http://localhost:3000](http://localhost:3000) for Grafana (`admin` / `admin`) and [http://localhost:8081](http://localhost:8081) for Redpanda Console. Port 8081 is used for Console to avoid conflicts with other local apps. The Grafana dashboard is provisioned automatically and refreshes every two seconds.

### Verify telemetry and troubleshoot

The simulator publishes 10 device readings once per second. Check simulator and processor logs:

```bash
kubectl logs -n iot-pipeline deployment/iot-simulator --tail=20
kubectl logs -n iot-pipeline deployment/stream-processor --tail=20
```

Check the number of readings saved in PostgreSQL:

```bash
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT count(*) AS readings FROM readings;"
```

If the dashboard panels say “No data,” check that both pods are Ready, that the simulator log says it connected to `redpanda:9092`, and that the dashboard time range includes the latest readings.

Pause and resume telemetry without stopping the rest of the stack:

```bash
kubectl scale deployment/iot-simulator -n iot-pipeline --replicas=0
kubectl scale deployment/iot-simulator -n iot-pipeline --replicas=1
```

To remove the deployment and its namespace, run:

```bash
kubectl delete namespace iot-pipeline
```

Deleting the namespace also deletes its PostgreSQL persistent volume claim and the data stored in that volume.

See [`k8s/README.md`](k8s/README.md) for the manifest reference and deployment architecture.
