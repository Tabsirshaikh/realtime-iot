"""
Stream Processor for IoT Pipeline (Two-Day Plan: Person 2 & Person 3).

Responsibilities:
- Person 2 (P2):
  * Consume from 'sensors.raw'
  * Validate payload schema and data integrity
  * Route defective payloads to PostgreSQL 'rejected' table
  * Insert valid clean records into PostgreSQL 'readings' table
  * Compute 1-minute tumbling window rollups and insert into 'agg_1m' table
- Person 3 (P3):
  * Rule 1: Temperature > 45 C OR Power > 10 kW
  * Rule 2: Dynamic 3-sigma anomaly rule (deviates > 3 std deviations from device's last 20 readings)
  * Emit anomalies to Redpanda 'sensors.alerts' and insert into PostgreSQL 'alerts' table
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [processor] %(message)s",
)
logger = logging.getLogger("processor")


class AnomalyEngine:
    """
    Person 3 Anomaly Detection Engine:
    1. Static bounds: temp > 45.0 C or power > 10.0 kW
    2. Dynamic 3-sigma rule: |val - mean| > 3 * std_dev over the device's last 20 readings.
    """

    def __init__(
        self,
        temp_threshold_c: float = 45.0,
        power_threshold_kw: float = 10.0,
        history_len: int = 20,
    ) -> None:
        self.temp_threshold_c = temp_threshold_c
        self.power_threshold_kw = power_threshold_kw
        self.history_len = history_len

        # device_id -> deque of recent floats
        self._temp_history: Dict[str, deque] = {}
        self._power_history: Dict[str, deque] = {}

    def check_anomalies(self, reading: Dict[str, Any]) -> List[Dict[str, Any]]:
        dev_id = reading["device_id"]
        temp = reading["temperature_c"]
        power = reading["power_kw"]
        ts = reading["event_ts"]

        if dev_id not in self._temp_history:
            self._temp_history[dev_id] = deque(maxlen=self.history_len)
            self._power_history[dev_id] = deque(maxlen=self.history_len)

        temp_hist = self._temp_history[dev_id]
        power_hist = self._power_history[dev_id]

        alerts = []

        # 1. Rule 1: Hard limits
        if temp > self.temp_threshold_c:
            alerts.append({
                "device_id": dev_id,
                "event_ts": ts,
                "type": "temp_spike_hard_limit",
                "value": temp,
            })
        if power > self.power_threshold_kw:
            alerts.append({
                "device_id": dev_id,
                "event_ts": ts,
                "type": "power_surge_hard_limit",
                "value": power,
            })

        # 2. Rule 2: Dynamic 3-sigma (requires at least 5 baseline readings)
        if len(temp_hist) >= 5:
            mean_t = sum(temp_hist) / len(temp_hist)
            variance_t = sum((x - mean_t) ** 2 for x in temp_hist) / len(temp_hist)
            std_t = math.sqrt(variance_t)
            if std_t > 0.05 and abs(temp - mean_t) > (3.0 * std_t):
                alerts.append({
                    "device_id": dev_id,
                    "event_ts": ts,
                    "type": "temp_3_sigma_anomaly",
                    "value": temp,
                })

        if len(power_hist) >= 5:
            mean_p = sum(power_hist) / len(power_hist)
            variance_p = sum((x - mean_p) ** 2 for x in power_hist) / len(power_hist)
            std_p = math.sqrt(variance_p)
            if std_p > 0.05 and abs(power - mean_p) > (3.0 * std_p):
                alerts.append({
                    "device_id": dev_id,
                    "event_ts": ts,
                    "type": "power_3_sigma_anomaly",
                    "value": power,
                })

        temp_hist.append(temp)
        power_hist.append(power)

        return alerts


class TumblingWindowAggregator:
    """
    Person 2 Window Engine:
    Maintains 1-minute tumbling windows per device and aggregates:
    (device_id, window_start, avg_temp, max_temp, avg_power, n)
    """

    def __init__(self, window_sec: int = 60) -> None:
        self.window_sec = window_sec
        # (device_id, window_start_str) -> {"temps": [], "powers": []}
        self.windows: Dict[Tuple[str, str], Dict[str, List[float]]] = {}

    def add_reading(self, reading: Dict[str, Any]) -> None:
        dev_id = reading["device_id"]
        ts_str = reading["event_ts"]
        
        # Parse timestamp to calculate window_start
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        epoch = int(dt.timestamp())
        start_epoch = (epoch // self.window_sec) * self.window_sec
        w_start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
        w_start_str = w_start_dt.isoformat()

        key = (dev_id, w_start_str)
        if key not in self.windows:
            self.windows[key] = {"temps": [], "powers": []}

        self.windows[key]["temps"].append(reading["temperature_c"])
        self.windows[key]["powers"].append(reading["power_kw"])

    def flush_expired(self, current_dt: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Flush windows older than 1 minute."""
        now = current_dt or datetime.now(timezone.utc)
        cutoff_epoch = int(now.timestamp()) - self.window_sec

        closed_aggregates = []
        keys_to_del = []

        for (dev_id, w_start_str), data in self.windows.items():
            w_dt = datetime.fromisoformat(w_start_str)
            if int(w_dt.timestamp()) <= cutoff_epoch:
                temps = data["temps"]
                powers = data["powers"]
                n = len(temps)
                if n > 0:
                    closed_aggregates.append({
                        "device_id": dev_id,
                        "window_start": w_start_str,
                        "avg_temp": round(sum(temps) / n, 2),
                        "max_temp": round(max(temps), 2),
                        "avg_power": round(sum(powers) / n, 3),
                        "n": n,
                    })
                keys_to_del.append((dev_id, w_start_str))

        for k in keys_to_del:
            del self.windows[k]

        return closed_aggregates

    def flush_all(self) -> List[Dict[str, Any]]:
        """Force flush all open windows."""
        closed = []
        for (dev_id, w_start_str), data in self.windows.items():
            temps = data["temps"]
            powers = data["powers"]
            n = len(temps)
            if n > 0:
                closed.append({
                    "device_id": dev_id,
                    "window_start": w_start_str,
                    "avg_temp": round(sum(temps) / n, 2),
                    "max_temp": round(max(temps), 2),
                    "avg_power": round(sum(powers) / n, 3),
                    "n": n,
                })
        self.windows.clear()
        return closed


class DatabaseClient:
    """
    PostgreSQL Client with automatic fallback to in-memory buffers when offline.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "iot_db",
        user: str = "iot_user",
        password: str = "iot_password",
        mock_mode: bool = False,
    ) -> None:
        self.mock_mode = mock_mode
        self.conn = None

        # In-memory tables for mock / offline testing
        self.mock_db = {
            "readings": [],
            "agg_1m": [],
            "alerts": [],
            "rejected": [],
        }

        if not self.mock_mode:
            try:
                import psycopg2
                self.conn = psycopg2.connect(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                    connect_timeout=3,
                )
                self.conn.autocommit = True
                logger.info(f"Connected to PostgreSQL at {host}:{port}/{dbname}")
            except Exception as e:
                logger.warning(f"PostgreSQL connection failed ({e}). Falling back to in-memory storage.")
                self.mock_mode = True

    def insert_reading(self, reading: Dict[str, Any]) -> None:
        if self.mock_mode or self.conn is None:
            self.mock_db["readings"].append(reading)
            return

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO readings (device_id, room, event_ts, temperature_c, power_kw)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    reading["device_id"],
                    reading["room"],
                    reading["event_ts"],
                    reading["temperature_c"],
                    reading["power_kw"],
                ),
            )

    def insert_rejected(self, raw_payload: str, reason: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        if self.mock_mode or self.conn is None:
            self.mock_db["rejected"].append({"raw_payload": raw_payload, "reason": reason, "ts": ts})
            return

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rejected (raw_payload, reason, ts)
                VALUES (%s, %s, %s);
                """,
                (raw_payload, reason, ts),
            )

    def insert_agg_1m(self, agg: Dict[str, Any]) -> None:
        if self.mock_mode or self.conn is None:
            self.mock_db["agg_1m"].append(agg)
            return

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agg_1m (device_id, window_start, avg_temp, max_temp, avg_power, n)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (
                    agg["device_id"],
                    agg["window_start"],
                    agg["avg_temp"],
                    agg["max_temp"],
                    agg["avg_power"],
                    agg["n"],
                ),
            )

    def insert_alert(self, alert: Dict[str, Any]) -> None:
        if self.mock_mode or self.conn is None:
            self.mock_db["alerts"].append(alert)
            return

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (device_id, event_ts, type, value)
                VALUES (%s, %s, %s, %s);
                """,
                (
                    alert["device_id"],
                    alert["event_ts"],
                    alert["type"],
                    alert["value"],
                ),
            )

    def close(self) -> None:
        if self.conn:
            self.conn.close()


def validate_raw_message(raw_msg: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Validates message against JSON contract:
    {device_id, room, event_ts, temperature_c, power_kw}
    """
    try:
        data = json.loads(raw_msg)
    except Exception as e:
        return None, f"Malformed JSON syntax: {str(e)}"

    if not isinstance(data, dict):
        return None, f"Expected JSON object, got {type(data).__name__}"

    required_fields = ["device_id", "room", "event_ts", "temperature_c", "power_kw"]
    for req in required_fields:
        if req not in data:
            return None, f"Missing required field '{req}'"
        if data[req] is None:
            return None, f"Field '{req}' cannot be null"

    try:
        data["device_id"] = str(data["device_id"])
        data["room"] = str(data["room"])
        data["temperature_c"] = float(data["temperature_c"])
        data["power_kw"] = float(data["power_kw"])
        # Validate ISO timestamp
        _ = datetime.fromisoformat(data["event_ts"].replace("Z", "+00:00"))
    except Exception as e:
        return None, f"Field type cast error: {str(e)}"

    if data["power_kw"] < 0:
        return None, f"power_kw cannot be negative ({data['power_kw']})"

    if not (-50.0 <= data["temperature_c"] <= 120.0):
        return None, f"temperature_c out of physical range ({data['temperature_c']})"

    return data, None


class StreamProcessor:
    """
    Main processing loop integrating Consumer, Validator, AnomalyEngine,
    TumblingWindowAggregator, and DatabaseClient.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        postgres_host: str = "localhost",
        postgres_port: int = 5432,
        postgres_db: str = "iot_db",
        postgres_user: str = "iot_user",
        postgres_password: str = "iot_password",
        mock_mode: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.mock_mode = mock_mode

        self.db = DatabaseClient(
            host=postgres_host,
            port=postgres_port,
            dbname=postgres_db,
            user=postgres_user,
            password=postgres_password,
            mock_mode=mock_mode,
        )

        self.anomaly_engine = AnomalyEngine()
        self.window_aggregator = TumblingWindowAggregator(window_sec=60)

        self.consumer = None
        self.producer = None
        self._init_kafka()

        # Operational statistics
        self.stats = {
            "received": 0,
            "readings_written": 0,
            "rejected_written": 0,
            "alerts_written": 0,
            "aggregates_written": 0,
        }

    def _init_kafka(self) -> None:
        if self.mock_mode:
            return

        try:
            from confluent_kafka import Consumer, Producer
            c_conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "processor-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
            self.consumer = Consumer(c_conf)
            self.consumer.subscribe(["sensors.raw"])

            p_conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "processor-alerts-producer",
            }
            self.producer = Producer(p_conf)
            logger.info("Kafka Consumer and Producer initialized successfully.")
        except Exception as e:
            logger.warning(f"Kafka initialization failed ({e}). Running in mock mode.")
            self.mock_mode = True

    def process_message_payload(self, raw_str: str) -> None:
        """Process one incoming message string end-to-end."""
        self.stats["received"] += 1

        # 1. Validation
        valid_reading, error_reason = validate_raw_message(raw_str)
        if error_reason:
            self.db.insert_rejected(raw_str, error_reason)
            self.stats["rejected_written"] += 1
            return

        assert valid_reading is not None

        # 2. Write to readings table
        self.db.insert_reading(valid_reading)
        self.stats["readings_written"] += 1

        # 3. Check anomalies (P3)
        alerts = self.anomaly_engine.check_anomalies(valid_reading)
        for alert in alerts:
            self.db.insert_alert(alert)
            self.stats["alerts_written"] += 1
            # Forward alert to sensors.alerts topic
            if self.producer:
                self.producer.produce(
                    "sensors.alerts",
                    key=alert["device_id"].encode("utf-8"),
                    value=json.dumps(alert).encode("utf-8"),
                )

        # 4. Add to 1-minute window rollup (P2)
        self.window_aggregator.add_reading(valid_reading)

    def flush_windows(self) -> None:
        """Check and flush expired 1-minute windows."""
        expired = self.window_aggregator.flush_expired()
        for agg in expired:
            self.db.insert_agg_1m(agg)
            self.stats["aggregates_written"] += 1

    def run(self, max_messages: int = 0) -> None:
        """Main consumer execution loop."""
        running = True

        def _handle_exit(sig, frame):
            nonlocal running
            logger.info("Stopping stream processor...")
            running = False

        signal.signal(signal.SIGINT, _handle_exit)
        signal.signal(signal.SIGTERM, _handle_exit)

        logger.info("Stream Processor listening for events on 'sensors.raw'...")
        start_time = time.time()
        last_flush = time.time()

        try:
            while running:
                if self.consumer:
                    msg = self.consumer.poll(timeout=0.5)
                    if msg is not None:
                        if not msg.error():
                            raw_val = msg.value().decode("utf-8") if msg.value() else ""
                            self.process_message_payload(raw_val)

                # Periodic window flush every 5 seconds
                if time.time() - last_flush >= 5.0:
                    self.flush_windows()
                    if self.producer:
                        self.producer.flush(0.1)
                    last_flush = time.time()

                if max_messages > 0 and self.stats["received"] >= max_messages:
                    logger.info(f"Target count of {max_messages} messages reached. Exiting.")
                    break

        finally:
            # Force flush remaining open windows
            final_aggs = self.window_aggregator.flush_all()
            for agg in final_aggs:
                self.db.insert_agg_1m(agg)
                self.stats["aggregates_written"] += 1

            if self.producer:
                self.producer.flush(2.0)
            if self.consumer:
                self.consumer.close()
            self.db.close()

            elapsed = time.time() - start_time
            logger.info("================ PROCESSOR SUMMARY ================")
            logger.info(f"Runtime: {elapsed:.2f} s")
            logger.info(f"Total Received:         {self.stats['received']}")
            logger.info(f"Readings Saved:         {self.stats['readings_written']}")
            logger.info(f"Rejected Rows:          {self.stats['rejected_written']}")
            logger.info(f"Alerts Triggered:       {self.stats['alerts_written']}")
            logger.info(f"1-min Rollups Saved:    {self.stats['aggregates_written']}")
            logger.info("===================================================")


def main() -> None:
    parser = argparse.ArgumentParser(description="IoT Stream Processor (P2 + P3)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--postgres-host", default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--postgres-db", default="iot_db")
    parser.add_argument("--postgres-user", default="iot_user")
    parser.add_argument("--postgres-password", default="iot_password")
    parser.add_argument("--mock", action="store_true", help="Run in mock/offline mode")
    parser.add_argument("--max-messages", type=int, default=0)

    args = parser.parse_args()

    processor = StreamProcessor(
        bootstrap_servers=args.bootstrap_servers,
        postgres_host=args.postgres_host,
        postgres_port=args.postgres_port,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        mock_mode=args.mock,
    )
    processor.run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
