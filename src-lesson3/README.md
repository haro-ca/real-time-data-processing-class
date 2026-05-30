# Lesson 3 — Why is OLAP a fundamentally different problem?

DuckDB vs Postgres on 100M NYC taxi rows. Same hardware, 67× faster.

## Quick start

```bash
# 1. Download data (~1.5 GB total)
chmod +x download_data.sh && ./download_data.sh

# 2. Start Postgres (4 CPU, 8 GB — containerized)
docker compose up -d

# 3. Install Python deps
uv sync

# 4. Load data into Postgres (takes a few minutes — that's the point)
uv run python load_postgres.py

# 5. Run the benchmark
uv run python benchmark_queries.py
```

## Scripts

| Script | Purpose |
|--------|---------|
| `download_data.sh` | Fetch 2023 NYC Yellow Taxi Parquet files |
| `load_postgres.py` | Load Parquet → Postgres via DuckDB CSV export + COPY |
| `benchmark_queries.py` | 4 queries head-to-head (PG vs DuckDB), wall-clock times |
| `experiment_sharded_copy.py` | Sharded parallel COPY on CockroachDB |
| `experiment_point_query.py` | Point lookup comparison (PG wins) |
| `experiment_ordering.py` | Sorted vs shuffled data — zone map effectiveness |

## Sharding demo

The sharding experiment runs against the CockroachDB cluster included in this lesson's docker-compose:

```bash
# Start both Postgres and CockroachDB
docker compose up -d

# Run sharded COPY
uv run python experiment_sharded_copy.py --ranges 8 --conns 8 --rows 100000
```

## Container resources

Both Postgres and DuckDB run with **4 CPU, 8 GB RAM** for fair comparison.
DuckDB runs in-process (via Python) — the Docker resource limits apply to the
Python container if you use `docker compose exec`.

For the native (unconstrained) DuckDB comparison, just run directly:
```bash
uv run python benchmark_queries.py --duck-only
```
