# End-to-End Testing Cheat Sheet

Commands below are for the project's Kubernetes deployment from the repository root in **PowerShell**. The live stream is Redpanda → Python processor → PostgreSQL → Grafana. Parquet, DuckDB, Polars, and PySpark are a separate, manually run historical analytics path.

## 1. Check the Kubernetes stack

```powershell
kubectl config current-context
kubectl get pods -n iot-pipeline -o wide
kubectl get svc -n iot-pipeline
```

Expected deployments/pods include `redpanda`, `stream-processor`, `postgres`, `iot-simulator`, `redpanda-console`, and `grafana`. Core pods should be `Running` and ready. The topic-init job may show `Completed`.

If a pod is not healthy, inspect its logs and events:

```powershell
kubectl logs -n iot-pipeline deployment/redpanda --tail=80
kubectl logs -n iot-pipeline deployment/stream-processor --tail=80
kubectl logs -n iot-pipeline deployment/postgres --tail=80
kubectl logs -n iot-pipeline deployment/iot-simulator --tail=80
kubectl get events -n iot-pipeline --sort-by=.lastTimestamp | Select-Object -Last 20
```

## 2. Verify Redpanda topics and incoming events

List topics and inspect topic traffic:

```powershell
kubectl exec -n iot-pipeline deployment/redpanda -- rpk topic list
kubectl exec -n iot-pipeline deployment/redpanda -- rpk topic describe sensors.raw
kubectl exec -n iot-pipeline deployment/redpanda -- rpk topic describe sensors.alerts
```

Expected topics: `sensors.raw` (incoming readings) and `sensors.alerts` (anomaly events). `sensors.raw` offsets should increase while the simulator is running.

View them in the browser using the project's Redpanda Console port-forward:

```powershell
kubectl port-forward -n iot-pipeline svc/redpanda-console 8081:8080
```

Open `http://localhost:8081`, choose **Topics**, then inspect `sensors.raw` or `sensors.alerts`. Keep the port-forward terminal open while using the page.

## 3. Verify PostgreSQL stream processing

Read the processor logs and query the database tables:

```powershell
kubectl logs -n iot-pipeline deployment/stream-processor --tail=80
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS readings FROM readings;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS alerts FROM alerts;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS rejected_rows FROM rejected;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS minute_rollups FROM agg_1m;"
```

Inspect recent records:

```powershell
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT device_id, room, event_ts, temperature_c, power_kw FROM readings ORDER BY event_ts DESC LIMIT 10;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT device_id, event_ts, type, value FROM alerts ORDER BY event_ts DESC LIMIT 10;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT ts, reason, raw_payload FROM rejected ORDER BY ts DESC LIMIT 10;"
```

Healthy normal flow means readings and rollups accumulate. Alerts and rejected rows can remain zero until you run the corresponding injected test below. Tables are `readings`, `agg_1m`, `alerts`, and `rejected`.

## 4. Test anomaly handling and rejected rows

The Kubernetes simulator runs continuously. Scale it down before starting an additional simulator, so the test stream is easier to identify:

```powershell
kubectl scale deployment/iot-simulator -n iot-pipeline --replicas=0
```

Create or refresh a ConfigMap from the repository simulator, then launch it as a temporary pod in the cluster. `--spike` injects values above the project's hard thresholds; `--bad-rows` injects malformed JSON, missing/null fields, and negative power values.

```powershell
kubectl create configmap simulator-source -n iot-pipeline --from-file=simulator.py --dry-run=client -o yaml | kubectl apply -f -
kubectl run iot-test-spikes -n iot-pipeline --restart=Never --image=python:3.11-slim --env="PYTHONUNBUFFERED=1" --command -- sh -c "pip install --quiet confluent-kafka && python /app/simulator.py --bootstrap-servers redpanda:9092 --devices 5 --rate 0.2 --duration 20 --spike"
```

The `kubectl run` command needs the simulator source mounted at `/app/simulator.py`. If there is no existing test pod template, run the project simulator by temporarily changing the deployed simulator's arguments to include `--spike` or `--bad-rows`, then restore the original deployment arguments afterward. One safe repeatable route is to run the producer from a local Python environment with the Kafka bootstrap address available to the host; this project's in-cluster broker advertises `redpanda:9092`, so a host port-forward may not work for producer traffic.

For the currently deployed simulator, enable either injection mode by editing its Deployment:

```powershell
kubectl edit deployment/iot-simulator -n iot-pipeline
```

Append `--spike` or `--bad-rows` to the container's `args`, save, and let the pod restart. After a short run, verify the counters and samples:

```powershell
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS alerts FROM alerts;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT COUNT(*) AS rejected_rows FROM rejected;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT type, COUNT(*) FROM alerts GROUP BY type ORDER BY type;"
kubectl exec -n iot-pipeline deployment/postgres -- psql -U iot_user -d iot_db -c "SELECT reason, COUNT(*) FROM rejected GROUP BY reason ORDER BY reason;"
```

Expected: spike tests increase `alerts`; bad-row tests increase `rejected`. Clean readings continue to go to `readings`. The injection flags are probabilistic, so leave the test running long enough to produce samples. Remove the flag from the Deployment when done, then resume the regular simulator if it was scaled down:

```powershell
kubectl scale deployment/iot-simulator -n iot-pipeline --replicas=1
```

## 5. Verify Grafana

In a separate terminal:

```powershell
kubectl port-forward -n iot-pipeline svc/grafana 3000:3000
```

Open `http://localhost:3000` and sign in with the configured project credentials (`admin` / `admin`, unless changed). Open the provisioned **Smart Building Real-Time IoT Dashboard**. Set the time range to **Last 15 minutes** and refresh. Check that total clean readings, throughput, alerts, and rejected-row panels agree with PostgreSQL, and that room temperature/power charts have points. A zero alert/rejected count is normal without injection tests.

If panels show no data, confirm the simulator and processor are running, query recent `readings`, and use a time range that includes their timestamps.

## 6. Generate and validate Parquet analytics data

DuckDB, Polars, and PySpark read a historical Parquet file; they do not automatically run as Kubernetes services. Generate a reproducible-size synthetic dataset locally:

```powershell
python -m pip install -r requirements.txt
python make_history.py --rows 100000 --devices 20 --output history.parquet
Get-Item .\history.parquet | Select-Object Name, Length
```

The generator writes timestamped readings to Parquet and includes some high temperature/power anomaly values. To export the live PostgreSQL readings instead, first forward PostgreSQL in a separate terminal:

```powershell
kubectl port-forward -n iot-pipeline svc/postgres 5432:5432
```

Then, in the repo terminal:

```powershell
python make_history.py --export-postgres --output history_live.parquet
```

That export needs at least one row in PostgreSQL's `readings` table. The default output for the analytics examples below is `history.parquet`.

## 7. Run DuckDB and Polars comparison

```powershell
python analytics.py --parquet history.parquet --runs 3
```

The script runs four equivalent queries with DuckDB and Polars: hourly room temperature summaries, daily room power totals, top anomalous devices, and readings per device. Check the output for result row counts/parity messages and the timing comparison table. Timings depend on dataset size, machine load, and cache state; a single run is not a general performance claim.

## 8. Run PySpark analytics on the same Parquet file

```powershell
python spark_analytics.py --parquet history.parquet
```

Expected output includes row count, four query summaries, and caching/partition timings. This project configures Spark in local mode (`local[*]`); it demonstrates Spark DataFrame transformations and actions on your machine, not a separate Spark Kubernetes cluster. If Java/Spark cannot start, this script may print that it is using its Polars-based Spark lifecycle emulation. Treat that output as an emulation, not a real PySpark execution. For an actual PySpark run, install a compatible Java runtime and PySpark first.

## 9. End-to-end pass checklist

- [ ] Kubernetes pods for Redpanda, processor, PostgreSQL, simulator, Console, and Grafana are healthy.
- [ ] Redpanda has `sensors.raw` and `sensors.alerts`; raw topic offsets increase.
- [ ] PostgreSQL `readings` and `agg_1m` receive records.
- [ ] Spike injection causes alert records; bad-row injection causes rejected records.
- [ ] Grafana displays recent readings and reflects alert/rejected tests.
- [ ] `history.parquet` is generated and readable by the analytics scripts.
- [ ] DuckDB and Polars complete their four-query comparison.
- [ ] PySpark output confirms a real Spark session, or is clearly identified as emulation if Java is unavailable.

## 10. Useful cleanup

Remove only the local historical files generated by the commands above when you no longer need them:

```powershell
Remove-Item .\history.parquet, .\history_live.parquet -ErrorAction SilentlyContinue
```

Stop Kubernetes port-forwards with **Ctrl+C** in their terminals. To stop the project's Kubernetes stack entirely, run `kubectl delete namespace iot-pipeline` (this also removes the namespace's PostgreSQL persistent storage and its data).
