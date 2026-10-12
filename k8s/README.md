# Kubernetes Deployment Guide (Bonus Deliverable)

This directory contains production-ready Kubernetes manifests to run the entire Real-Time IoT Stream Processing Pipeline in a Kubernetes cluster (`Minikube`, `Kind`, `K3s`, or managed cloud Kubernetes).

---

## 1. Architecture on Kubernetes

```
                  [simulator.py (External or Pod)]
                                |
                                v
               +-----------------------------------+
               |  Service: redpanda:9092           |
               |  Pod: redpanda (Stateful broker)  |
               +-----------------------------------+
                                |
                                v
               +-----------------------------------+
               |  Pod: stream-processor            |
               |  - Schema Validation              |
               |  - 3-Sigma Anomaly Detection      |
               |  - 1-Minute Tumbling Windows      |
               +-----------------------------------+
                                |
                                v
               +-----------------------------------+
               |  Service: postgres:5432           |
               |  Pod: postgres (Persistent PVC)   |
               +-----------------------------------+
                                |
                                v
               +-----------------------------------+
               |  Service: grafana:3000 (NodePort) |
               |  Pod: grafana (Live Dashboard)    |
               +-----------------------------------+
```

---

## 2. Manifest Overview

| File | Purpose |
|---|---|
| `namespace.yaml` | Creates isolated `iot-pipeline` namespace |
| `postgres.yaml` | Deploys PostgreSQL 16 with schema ConfigMap, PVC, and ClusterIP service |
| `redpanda.yaml` | Deploys Redpanda Kafka broker with topic initialization job |
| `processor.yaml` | Deploys Python Stream Processor connected to internal services |
| `simulator.yaml` | Publishes live telemetry from a simulator pod to Redpanda |
| `grafana.yaml` | Deploys Grafana with auto-provisioned PostgreSQL datasource and NodePort 30030 |

---

## 3. Quickstart Deployment

### Step 1: Create Namespace and Storage
```bash
kubectl apply -f namespace.yaml
kubectl apply -f postgres.yaml
```

### Step 2: Deploy Redpanda Message Broker
```bash
kubectl apply -f redpanda.yaml
```

Wait until Redpanda and the topic init job finish:
```bash
kubectl get pods -n iot-pipeline -w
```

### Step 3: Deploy Grafana, Stream Processor, and Simulator
```bash
kubectl apply -f grafana.yaml
kubectl apply -f processor.yaml
kubectl create configmap simulator-source -n iot-pipeline --from-file=simulator.py --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f simulator.yaml
```

The simulator runs inside the cluster and publishes continuously to `redpanda:9092`. Its source ConfigMap is generated from the repository's `simulator.py`; recreate it after changing that file.

### Step 4: Verify Deployment
```bash
kubectl get all -n iot-pipeline
```

### Step 5: Access the Services Locally
Forward Grafana:
```bash
kubectl port-forward -n iot-pipeline svc/grafana 3000:3000
```
Forward Redpanda only when you need a Kafka client on your host machine:
```bash
kubectl port-forward -n iot-pipeline svc/redpanda-external 19092:9092
```

The in-cluster simulator is the recommended way to populate Grafana; it avoids host Kafka advertised-listener routing differences.
