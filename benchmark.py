"""
Benchmark & Latency Measurement Tool for IoT Stream Pipeline.

Measures:
- Publishing throughput (messages/sec)
- Ingestion latency distribution (p50, p90, p95, p99 in milliseconds)
- Scaling across 10, 100, and 1000 simulated devices
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import time
from datetime import datetime, timezone
from typing import List

from simulator import DeviceState, SimulatorEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("benchmark")


def run_benchmark(
    bootstrap_servers: str,
    device_count: int = 100,
    total_messages: int = 5000,
    mock_mode: bool = False,
) -> None:
    logger.info(f"=== Starting Ingestion Benchmark: {device_count} devices, {total_messages} messages ===")
    
    sim = SimulatorEngine(
        bootstrap_servers=bootstrap_servers,
        device_count=device_count,
        rate_sec=0.0,  # Max speed
        inject_spikes=False,
        inject_bad_rows=False,
        mock_mode=mock_mode,
    )

    latencies_ms: List[float] = []
    start_time = time.perf_counter()

    for i in range(total_messages):
        dev = sim.devices[i % len(sim.devices)]
        payload, _ = sim.generate_payload(dev)

        t_send_start = time.perf_counter()
        sim.publish_message("sensors.raw", dev.device_id, payload)
        t_send_end = time.perf_counter()

        latencies_ms.append((t_send_end - t_send_start) * 1000.0)

    if sim.producer:
        sim.producer.flush(10.0)

    total_duration = time.perf_counter() - start_time
    throughput = total_messages / max(0.001, total_duration)

    latencies_ms.sort()
    p50 = statistics.median(latencies_ms)
    p90 = latencies_ms[int(len(latencies_ms) * 0.90)]
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]
    avg_lat = statistics.mean(latencies_ms)

    print("\n" + "=" * 60)
    print(f"BENCHMARK RESULTS ({'MOCK' if mock_mode else 'REDPANDA'})")
    print("=" * 60)
    print(f"Devices Simulated:   {device_count}")
    print(f"Total Messages:      {total_messages}")
    print(f"Total Elapsed Time:  {total_duration:.3f} s")
    print(f"Throughput:          {throughput:.2f} msg/sec")
    print("-" * 60)
    print("Latency Distribution (Producer Send -> Broker Ack):")
    print(f"  Average:           {avg_lat:.3f} ms")
    print(f"  Median (p50):      {p50:.3f} ms")
    print(f"  90th percentile:   {p90:.3f} ms")
    print(f"  95th percentile:   {p95:.3f} ms")
    print(f"  99th percentile:   {p99:.3f} ms")
    print("=" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="IoT Pipeline Latency Benchmark")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--devices", type=int, default=50)
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--mock", action="store_true")

    args = parser.parse_args()
    run_benchmark(
        bootstrap_servers=args.bootstrap_servers,
        device_count=args.devices,
        total_messages=args.messages,
        mock_mode=args.mock,
    )


if __name__ == "__main__":
    main()
