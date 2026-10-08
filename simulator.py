"""
IoT Sensor Telemetry Simulator (Two-Day Plan).

Emits smart building readings to topic 'sensors.raw' conforming to the contract:
{
    "device_id": "sensor-01",
    "room": "Room-201",
    "event_ts": "2026-10-08T12:00:00.000Z",
    "temperature_c": 22.5,
    "power_kw": 1.2
}

Features:
- Realistic physical drift (Brownian walk)
- --spike flag: Injects temperature spikes (> 45 C) and power surges (> 10 kW)
- --bad-rows flag: Injects malformed JSON, missing fields, and nulls
- In-memory mock fallback when Kafka broker is unreachable
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [simulator] %(message)s",
)
logger = logging.getLogger("simulator")


class DeviceState:
    """Maintains realistic temperature and power state for a simulated sensor."""

    def __init__(self, device_id: str, room: str) -> None:
        self.device_id = device_id
        self.room = room
        # Normal baseline conditions
        self.current_temp = random.uniform(20.0, 24.0)
        self.current_power = random.uniform(0.6, 2.5)

    def next_reading(self) -> Dict[str, Any]:
        """Advance physical state with realistic drift."""
        # Temperature random drift (-0.1 to +0.15 C)
        self.current_temp += random.gauss(0.0, 0.08)
        self.current_temp = max(18.0, min(28.0, self.current_temp))

        # Power draw correlated with thermal demand and ambient fluctuation
        self.current_power += random.gauss(0.0, 0.05)
        self.current_power = max(0.2, min(4.5, self.current_power))

        now_utc = datetime.now(timezone.utc).isoformat()

        return {
            "device_id": self.device_id,
            "room": self.room,
            "event_ts": now_utc,
            "temperature_c": round(self.current_temp, 2),
            "power_kw": round(self.current_power, 3),
        }


class SimulatorEngine:
    """Manages publishing loop, device fleet, and fault injections."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        device_count: int = 10,
        rate_sec: float = 1.0,
        inject_spikes: bool = False,
        inject_bad_rows: bool = False,
        mock_mode: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.rate_sec = rate_sec
        self.inject_spikes = inject_spikes
        self.inject_bad_rows = inject_bad_rows
        self.mock_mode = mock_mode

        # Initialize virtual fleet
        self.devices = [
            DeviceState(
                device_id=f"sensor-{i:02d}",
                room=f"Room-{200 + (i % 5)}",
            )
            for i in range(1, device_count + 1)
        ]

        self.producer = None
        self._init_producer()

    def _init_producer(self) -> None:
        if self.mock_mode:
            logger.info("Running in mock mode. Messages will be logged to stdout.")
            return

        try:
            from confluent_kafka import Producer
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "iot-simulator",
                "linger.ms": 5,
                "acks": 1,
            }
            self.producer = Producer(conf)
            logger.info(f"Connected to Kafka/Redpanda at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(
                f"Failed to connect to broker ({e}). Falling back to mock stdout mode."
            )
            self.mock_mode = True

    def publish_message(self, topic: str, key: str, payload: Any) -> None:
        """Publish payload (dict or raw string) to topic."""
        if self.mock_mode or self.producer is None:
            log_line = payload if isinstance(payload, str) else json.dumps(payload)
            logger.info(f"[MOCK RAW] {topic} -> {log_line}")
            return

        if isinstance(payload, str):
            val_bytes = payload.encode("utf-8")
        else:
            val_bytes = json.dumps(payload).encode("utf-8")

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=val_bytes,
        )
        self.producer.poll(0)

    def generate_payload(self, dev: DeviceState) -> Tuple[Any, str]:
        """
        Produces payload according to injection settings.
        Returns (payload, log_note).
        """
        reading = dev.next_reading()

        # 1. Check for bad-row injection
        if self.inject_bad_rows and random.random() < 0.25:
            bad_type = random.choice(["malformed_json", "missing_field", "null_field", "negative_power"])
            if bad_type == "malformed_json":
                return (
                    f'{{"device_id": "{dev.device_id}", "room": "{dev.room}", "temp": BROKEN_SYNTAX',
                    "BAD-ROW: Malformed JSON syntax",
                )
            elif bad_type == "missing_field":
                reading.pop("temperature_c", None)
                return reading, "BAD-ROW: Missing required 'temperature_c'"
            elif bad_type == "null_field":
                reading["power_kw"] = None
                return reading, "BAD-ROW: Null 'power_kw'"
            else:
                reading["power_kw"] = -99.9
                return reading, "BAD-ROW: Impossible negative power"

        # 2. Check for spike injection
        if self.inject_spikes and random.random() < 0.20:
            spike_type = random.choice(["temp_spike", "power_surge"])
            if spike_type == "temp_spike":
                # Temperature > 45 C rule
                reading["temperature_c"] = round(random.uniform(46.5, 65.0), 2)
                return reading, f"SPIKE: Critical Temperature {reading['temperature_c']} C (> 45 C)"
            else:
                # Power > 10 kW rule
                reading["power_kw"] = round(random.uniform(11.0, 24.5), 3)
                return reading, f"SPIKE: Power Surge {reading['power_kw']} kW (> 10 kW)"

        return reading, "NORMAL"

    def run(self, duration_sec: float = 0.0) -> None:
        """Run continuous simulation loop."""
        running = True

        def _handle_exit(sig, frame):
            nonlocal running
            logger.info("Stopping simulator gracefully...")
            running = False

        signal.signal(signal.SIGINT, _handle_exit)
        signal.signal(signal.SIGTERM, _handle_exit)

        start_time = time.time()
        sent_count = 0
        topic = "sensors.raw"

        logger.info(
            f"Simulator running! Emitting from {len(self.devices)} devices "
            f"every {self.rate_sec}s to '{topic}'..."
        )
        if self.inject_spikes:
            logger.info(">> Flag active: --spike (anomalous temperatures and power enabled)")
        if self.inject_bad_rows:
            logger.info(">> Flag active: --bad-rows (corrupted payloads enabled)")

        try:
            while running:
                t0 = time.time()

                for dev in self.devices:
                    if not running:
                        break

                    payload, note = self.generate_payload(dev)
                    self.publish_message(topic, dev.device_id, payload)
                    sent_count += 1

                    if note != "NORMAL" and not self.mock_mode:
                        logger.info(f"[{dev.device_id}] Injected {note}")

                if self.producer:
                    self.producer.flush(0.1)

                elapsed = time.time() - start_time
                if sent_count > 0 and int(elapsed) % 10 == 0:
                    logger.info(f"Published: {sent_count} readings ({sent_count / max(0.1, elapsed):.1f} msg/s)")

                if duration_sec > 0 and elapsed >= duration_sec:
                    logger.info(f"Reached specified duration ({duration_sec}s). Exiting.")
                    break

                sleep_time = max(0.0, self.rate_sec - (time.time() - t0))
                if sleep_time > 0 and running:
                    time.sleep(sleep_time)

        finally:
            if self.producer:
                self.producer.flush(3.0)
            logger.info(f"Simulation completed. Total messages sent: {sent_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="IoT Sensor Simulator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Broker bootstrap servers")
    parser.add_argument("--devices", type=int, default=10, help="Number of virtual devices")
    parser.add_argument("--rate", type=float, default=1.0, help="Interval per device in seconds")
    parser.add_argument("--spike", action="store_true", help="Inject temperature spikes (> 45C) and power surges (> 10kW)")
    parser.add_argument("--bad-rows", action="store_true", help="Inject corrupted payloads and missing fields")
    parser.add_argument("--duration", type=float, default=0.0, help="Duration in seconds (0 = infinite)")
    parser.add_argument("--mock", action="store_true", help="Run without Kafka broker (log output)")

    args = parser.parse_args()

    sim = SimulatorEngine(
        bootstrap_servers=args.bootstrap_servers,
        device_count=args.devices,
        rate_sec=args.rate,
        inject_spikes=args.spike,
        inject_bad_rows=args.bad_rows,
        mock_mode=args.mock,
    )
    sim.run(duration_sec=args.duration)


if __name__ == "__main__":
    main()
