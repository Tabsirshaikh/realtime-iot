-- IoT Stream Pipeline Schema
-- Auto-mounted into PostgreSQL /docker-entrypoint-initdb.d/init.sql

CREATE TABLE IF NOT EXISTS readings (
    device_id TEXT NOT NULL,
    room TEXT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    power_kw DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS agg_1m (
    device_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    avg_temp DOUBLE PRECISION NOT NULL,
    max_temp DOUBLE PRECISION NOT NULL,
    avg_power DOUBLE PRECISION NOT NULL,
    n INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected (
    id SERIAL PRIMARY KEY,
    raw_payload TEXT NOT NULL,
    reason TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for low-latency Grafana querying
CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_readings_dev_ts ON readings (device_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_agg_1m_start ON agg_1m (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_rejected_ts ON rejected (ts DESC);
