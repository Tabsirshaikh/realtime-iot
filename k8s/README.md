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

### Step 3: Deploy Grafana and Stream Processor
```bash
kubectl apply -f grafana.yaml
kubectl apply -f processor.yaml
```

### Step 4: Verify Deployment
```bash
kubectl get all -n iot-pipeline
```

### Step 5: Access the Services Locally
Forward Grafana:
```bash
kubectl port-forward -n iot-pipeline svc/grafana 3000:3000
```
Forward Redpanda (to run simulator from your host machine):
```bash
kubectl port-forward -n iot-pipeline svc/redpanda 9092:9092
```

Then run the simulator from your local machine:
```bash
python simulator.py --bootstrap-servers localhost:9092 --devices 10 --spike
```
