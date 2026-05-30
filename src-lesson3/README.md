# Lesson 3 — Why is OLAP a fundamentally different problem?

DuckDB vs Postgres on ~11M NYC taxi rows. Both engines containerized at the
same cap (4 CPU / 8 GB). Scale up the data when you want bigger fireworks.

## Quick start

```bash
# 1. Make sure OrbStack / Docker Desktop is running
# 2. Download Q1 2025 data (~11M rows, ~150 MB)
./download_data.sh

# 3. Bring up the whole stack: Postgres, CockroachDB (3 nodes), DuckDB runner
docker compose up -d

# 4. Load Postgres (streams via DuckDB postgres extension — no CSV intermediate)
./bench python load_postgres.py

# 5. Run the head-to-head benchmark
./bench python benchmark_queries.py
```

`./bench <cmd...>` is a thin wrapper over `docker compose exec duckdb <cmd...>` —
it runs the script inside the duckdb container where the 4 CPU / 8 GB cap matches
Postgres. For the "lift the cage" reveal at the end of the lesson, run natively:

```bash
uv run python benchmark_queries.py --duck-only
```

## Scripts

| Script | Purpose |
|--------|---------|
| `download_data.sh` | Q1 2025 by default (~11M rows). Pass `stretch` for full 2023-2025 (~128M). |
| `download_data_1B.sh` | Easter egg: full 2009-2025 archive (~1.5B rows, ~25 GB) |
| `load_postgres.py` | Stream Parquet → Postgres COPY via DuckDB's `postgres` extension |
| `benchmark_queries.py` | 4 queries head-to-head (PG vs DuckDB), wall-clock times |
| `experiment_point_query.py` | Point lookup comparison (PG wins ~700×) |
| `experiment_ordering.py` | Sorted vs shuffled data — zone-map selectivity gradient |
| `experiment_sharded_copy.py` | Sharded parallel COPY on CockroachDB |

## Scale ladder

The dataset is base-10 friendly so you can carry the mental model through bigger demos:

```
10M rows   ./download_data.sh             ~150 MB parquet, ~1.5 GB pg heap, 20 s load
100M rows  ./download_data.sh stretch     ~2 GB parquet,   ~17 GB pg heap,  ~7 min load
1B rows    ./download_data_1B.sh          ~25 GB parquet,  ~150 GB pg heap, hours
```

At 10M the head-to-head ratios are 4-23×. At 128M the same code measures 19-103×.
Same architecture, the gap widens with scale — that's the point.

## Container resources

- **Postgres**: 4 CPU, 8 GB RAM (cgroup). `shared_buffers=2GB`, `work_mem=256MB`.
- **DuckDB**: 4 CPU, 8 GB RAM (cgroup), same as Postgres. Built from `Dockerfile`.
- **CockroachDB**: 3 nodes × (2 CPU, 4 GB) for the sharding demo only.

DuckDB is normally a Python library, not a container. We containerize it here
*only* so the head-to-head is fair. The native run is the reveal.

## Sharding demo (CockroachDB)

The 3-node CockroachDB cluster lives in this lesson's `docker-compose.yml`:

```bash
./bench python experiment_sharded_copy.py --ranges 8 --conns 8 --rows 100000
```

> **Local result**: 3 CRDB nodes on one laptop share the same disk and CPU, so
> "sharding beats single-node" is hard to demonstrate locally — you'll typically
> measure the opposite. The demo shows the *mechanism* (each connection writes
> to a disjoint key range, no cross-range coordination), not a TPS win.

## Schema notes

The NYC TLC parquet schema drifted across years:

- 2023: `airport_fee` (lowercase), 19 columns
- 2024: `Airport_fee` (title case), 19 columns
- 2025: adds `cbd_congestion_fee` → 20 columns

The loader uses DuckDB's `union_by_name=true` to absorb both differences. Older
years get `NULL` for `cbd_congestion_fee`. The Postgres `trips` table is the
20-column 2025 superset.

## Troubleshooting

- **Host disk fills up**: the 128M stretch needs ~17 GB in OrbStack's VM disk
  on top of the parquet files. Free host space first.
- **Postgres connection refused from `./bench`**: container resolves Postgres
  by service DNS `postgres:5432`. If you renamed the service, set `PG_HOST` in
  the compose `environment:`.
- **`./bench: command not found`**: `chmod +x bench` then run with the leading
  `./`.
