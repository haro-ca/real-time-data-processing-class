#!/bin/bash

log() { echo "[demo] $*"; }

# Start Redpanda
redpanda start \
  --smp=1 --memory=512M --reserve-memory=0M --overprovisioned \
  --node-id=0 --check=false \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://localhost:9092 &

# Start ClickHouse
clickhouse server &

# Wait for Redpanda
log "Waiting for Redpanda..."
until rpk cluster health 2>/dev/null | grep -q "Healthy.*true"; do sleep 2; done
rpk topic create taps --partitions 1 --replicas 1 2>/dev/null || true
log "Redpanda ready"

# Wait for ClickHouse
log "Waiting for ClickHouse..."
until curl -sf http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; do sleep 2; done

# Init schema — swap docker-compose service name for localhost
sed 's/redpanda:9092/localhost:9092/g' /init.sql | clickhouse client --multiquery || true
log "ClickHouse ready"

export KAFKA_BOOTSTRAP=localhost:9092
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123

log "Starting FastAPI..."
exec uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
