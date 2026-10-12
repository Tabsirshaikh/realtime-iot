"""
Generates visual diagrams and dashboard mockups for academic submission documentation.
Saves high-resolution PNGs into docs/screenshots/.
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches

SCREENSHOTS_DIR = os.path.join(os.path.dirname(__file__), "docs", "screenshots")
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)


def draw_architecture_diagram():
    fig, ax = plt.subplots(figsize=(12, 6.5), dpi=200)
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 7)
    ax.axis("off")

    # Background
    fig.patch.set_facecolor("#f8fafc")
    ax.set_facecolor("#f8fafc")

    # Title
    ax.text(6, 6.6, "Real-Time IoT Stream Processing Pipeline (Topic 17 Architecture)", 
            ha="center", va="center", fontsize=15, fontweight="bold", color="#0f172a")

    # Boxes styling
    def draw_box(x, y, w, h, title, subtitle, color, border):
        box = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.15",
            ec=border, fc=color, lw=2
        )
        ax.add_patch(box)
        ax.text(x + w/2, y + h*0.65, title, ha="center", va="center", fontsize=11, fontweight="bold", color="#0f172a")
        ax.text(x + w/2, y + h*0.35, subtitle, ha="center", va="center", fontsize=8.5, color="#334155")

    # Flow Elements
    # 1. Simulator & Live API
    draw_box(0.5, 4.0, 2.4, 1.4, "Telemetry Sources", "simulator.py (Synthetic)\nlive_api_producer.py (Open-Meteo)", "#e0f2fe", "#0284c7")
    
    # 2. Redpanda Message Broker
    draw_box(3.6, 4.0, 2.4, 1.4, "Redpanda Broker", "sensors.raw (Partitioned p=3)\nsensors.alerts (Topic)", "#fee2e2", "#ef4444")
    
    # 3. Stream Processor
    draw_box(6.7, 4.0, 2.4, 1.4, "Stream Processor", "Schema Validation\n3-Sigma & Hard Limits\n1-Min Tumbling Windows", "#fef3c7", "#f59e0b")

    # 4. PostgreSQL Hot Store
    draw_box(9.6, 4.8, 2.0, 1.3, "PostgreSQL", "readings | agg_1m\nalerts | rejected", "#e0e7ff", "#6366f1")

    # 5. Grafana Dashboard
    draw_box(9.6, 2.8, 2.0, 1.3, "Grafana Control", "Real-Time Gauges\nLive Alerts & Anomaly Feed", "#ffedd5", "#f97316")

    # 6. Cold Storage & Analytical Engines
    draw_box(4.8, 1.0, 2.6, 1.3, "Historical Parquet", "make_history.py\nColumnar Storage (Snappy)", "#dcfce7", "#10b981")
    draw_box(8.2, 1.0, 3.4, 1.3, "Batch Query Race", "DuckDB (Vectorized SQL)\nPolars (Rust Multi-Threaded)\nPySpark (Distributed Plan)", "#f3e8ff", "#a855f7")

    # Arrows
    def draw_arrow(x1, y1, x2, y2, label=""):
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(facecolor="#475569", edgecolor="#475569", width=1.5, headwidth=7)
        )
        if label:
            ax.text((x1+x2)/2, (y1+y2)/2 + 0.15, label, ha="center", fontsize=8, color="#475569", fontweight="bold")

    draw_arrow(2.9, 4.7, 3.6, 4.7, "JSON Stream")
    draw_arrow(6.0, 4.7, 6.7, 4.7, "Kafka Poll")
    draw_arrow(9.1, 5.4, 9.6, 5.4, "Clean & Aggs")
    draw_arrow(10.6, 4.8, 10.6, 4.1, "SQL Queries")
    draw_arrow(7.9, 4.0, 6.1, 2.3, "Daily Export")
    draw_arrow(7.4, 1.65, 8.2, 1.65, "Zero-Copy Scan")

    fig.tight_layout()
    out_path = os.path.join(SCREENSHOTS_DIR, "architecture_diagram.png")
    fig.savefig(out_path)
    plt.close(fig)
    print(f"Saved: {out_path}")


def draw_grafana_mockup():
    fig, ax = plt.subplots(figsize=(11, 6), dpi=200)
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 6)
    ax.axis("off")
    fig.patch.set_facecolor("#111827")
    ax.set_facecolor("#111827")

    # Header
    ax.text(0.5, 5.6, "IoT Live Telemetry Control Room - Grafana Dashboard", fontsize=14, fontweight="bold", color="#f9fafb")
    ax.text(0.5, 5.25, "Data Source: PostgreSQL 16 Hot Store | Refresh: 1s | Topic: sensors.raw", fontsize=9, color="#9ca3af")

    # Panel 1: Live Temperature Gauge
    box1 = patches.FancyBboxPatch((0.5, 2.7), 3.0, 2.2, boxstyle="round,pad=0.08", ec="#374151", fc="#1f2937", lw=1.5)
    ax.add_patch(box1)
    ax.text(2.0, 4.5, "Avg Room Temperature", ha="center", fontsize=10, fontweight="bold", color="#9ca3af")
    ax.text(2.0, 3.7, "22.4 °C", ha="center", fontsize=24, fontweight="bold", color="#10b981")
    ax.text(2.0, 3.1, "Threshold: Normal (< 45°C)", ha="center", fontsize=8.5, color="#6b7280")

    # Panel 2: Live Alerts Counter
    box2 = patches.FancyBboxPatch((3.8, 2.7), 3.2, 2.2, boxstyle="round,pad=0.08", ec="#374151", fc="#1f2937", lw=1.5)
    ax.add_patch(box2)
    ax.text(5.4, 4.5, "Active Anomaly Alerts (3-Sigma)", ha="center", fontsize=10, fontweight="bold", color="#9ca3af")
    ax.text(5.4, 3.7, "6 Alerts", ha="center", fontsize=24, fontweight="bold", color="#ef4444")
    ax.text(5.4, 3.1, "Rule: |val - mean| > 3σ OR temp > 45°C", ha="center", fontsize=8.5, color="#6b7280")

    # Panel 3: Rejected Bad Rows
    box3 = patches.FancyBboxPatch((7.3, 2.7), 3.2, 2.2, boxstyle="round,pad=0.08", ec="#374151", fc="#1f2937", lw=1.5)
    ax.add_patch(box3)
    ax.text(8.9, 4.5, "Corrupted Payloads Rejected", ha="center", fontsize=10, fontweight="bold", color="#9ca3af")
    ax.text(8.9, 3.7, "15 Rows", ha="center", fontsize=24, fontweight="bold", color="#f59e0b")
    ax.text(8.9, 3.1, "Routed to Dead-Letter 'rejected' Table", ha="center", fontsize=8.5, color="#6b7280")

    # Panel 4: 1-Minute Tumbling Window Temperature Trend
    box4 = patches.FancyBboxPatch((0.5, 0.4), 10.0, 2.0, boxstyle="round,pad=0.08", ec="#374151", fc="#1f2937", lw=1.5)
    ax.add_patch(box4)
    ax.text(0.8, 2.1, "Aggregated 1-Minute Window Trend (agg_1m Table)", fontsize=10, fontweight="bold", color="#9ca3af")
    
    # Draw simulated trend lines
    x_pts = [1.0, 2.5, 4.0, 5.5, 7.0, 8.5, 10.0]
    y_pts = [1.0, 1.2, 1.1, 1.5, 1.3, 1.6, 1.4]
    ax.plot(x_pts, y_pts, color="#3b82f6", linewidth=2.5, marker="o", label="Avg Temp (°C)")
    y_spike = [1.3, 1.4, 1.3, 1.9, 1.5, 2.0, 1.7]
    ax.plot(x_pts, y_spike, color="#ef4444", linewidth=1.5, linestyle="--", label="Max Temp Spike")
    ax.legend(loc="lower right", facecolor="#1f2937", edgecolor="#374151", labelcolor="#f9fafb")

    fig.tight_layout()
    out_path = os.path.join(SCREENSHOTS_DIR, "grafana_dashboard_preview.png")
    fig.savefig(out_path)
    plt.close(fig)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    draw_architecture_diagram()
    draw_grafana_mockup()
