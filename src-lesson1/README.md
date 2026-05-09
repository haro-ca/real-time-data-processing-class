# Lesson 1 — How much can a single OLTP node handle?

PostgreSQL internals, load generation, and bottleneck analysis.

## Prerequisites

- Docker (with Compose v2)
- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) for dependency management
- `psql` (for interactive queries during demos)

## Quick start

```bash
# Start Postgres (constrained: 2 CPU, 4GB RAM)
docker compose up -d

# Install Python dependencies
uv sync

# Verify connection
psql "postgresql://bench:bench@localhost:5432/bench" -c "SELECT 1"
```

## Lesson flow

### Hour 1 — Theory demos (run before the corresponding slide)

| Slide | Script | What it sets up |
|-------|--------|-----------------|
| 5  | `python demos/demo_connections.py` | Opens 20 connections, some active. Query `pg_stat_activity` in psql. |
| 9  | `python demos/demo_buffers.py`     | Loads 50k rows. Query `pg_buffercache` in psql. |
| 13 | `python demos/demo_locks.py`       | N coroutines UPDATE same row. Watch lock waits in psql. |

All demo scripts hold state until Ctrl+C. They print the psql query to run.

### Hour 2 — Load generation phases

```bash
# Phase 1: Naive — single connection, synchronous
python load_naive.py --rows 100000

# Reset between phases
./reset.sh

# Phase 2: Async — asyncpg pool with N connections
python load_async.py --connections 50 --rows 100000

# Phase 3: Instrument — open a second terminal
# Run load_async.py in one, run instrument.sql queries in psql in the other

# Phase 4: Articulate the bottleneck (discussion, no script)
```

### Hour 3 — Experiments

```bash
./reset.sh

# Experiment A: Batching
python experiment_batch.py --rows 100000 --batch-size 1000
python experiment_batch.py --rows 100000 --batch-size 1000 --method copy

# Experiment B: synchronous_commit = off
python load_async.py --connections 50 --rows 100000 --no-sync

# Experiment C: Hot row contention
python experiment_hotrow.py --connections 50 --duration 30

# Experiment D: Bloat (in psql)
#   ALTER TABLE orders SET (autovacuum_enabled = false);
#   Then run: python load_async.py --connections 50 --rows 200000 --mode update
#   Then check: instrument.sql → "Dead tuples" query
#   Then run: VACUUM VERBOSE orders;
```

## Clean baseline

Between phases/experiments:

```bash
./reset.sh              # Truncate table + reset stats (fast)
docker compose restart  # Full restart if you want clean shared_buffers/WAL
```

## Instrumentation queries

All diagnostic queries are in `instrument.sql`. Copy-paste into psql as needed.

## Flame graph (requires py-spy)

```bash
uv sync --extra dev
sudo py-spy record -o flamegraph.svg -- python load_async.py --connections 50 --rows 50000
open flamegraph.svg
```
