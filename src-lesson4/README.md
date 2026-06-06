# Lesson 4 — Classical batch ETL and why "just move the data" is hard

Build a batch pipeline in raw python, watch it break, then make it correct. The
same pipeline is then wrapped in **Airflow** and **Dagster** — both runnable in
docker-compose — so you can map every framework concept back to the code you
wrote by hand.

- **Source (OLTP):** Postgres with `orders` (fact) + `customers` (mutable dimension).
- **Target (analytical):** DuckDB file `data/analytics.duckdb` (`daily_revenue`, `customers_dim`, `pipeline_metadata`).
- **Transform engine:** DuckDB (the "T" with ELT performance, technically ETL).

## Quick start

Two tools only: **docker compose** to start the infra, **`uv run`** to run the
Python scripts. No build tool to learn.

```bash
# 1. Start the infra: Postgres + Airflow + Dagster (needs Docker / OrbStack)
docker compose up -d

# 2. Seed the OLTP source (1M orders over 90 days, 50k customers)
uv run python src/seed_data.py

# 3. Hour 2 — feel the pain, then fix it
uv run python src/prove_idempotent.py 2024-01-15 --loader naive   # row count GROWS (not idempotent)
uv run python src/prove_idempotent.py 2024-01-15                  # idempotent: identical checksum ×3
uv run python src/pipeline_failure.py 2024-01-15 --fail-prob 0.4  # crash mid-load + retry → one result
uv run python src/pipeline_watermark.py 2024-01-10 2024-01-20     # multi-date load + atomic watermark

# 4. Dimensions + schema drift
uv run python src/scd2.py --merge --effective-date 2024-02-01     # initial SCD2 load
uv run python src/scd2.py --move-id 42                            # move customer 42 (→ Austin)
uv run python src/scd2.py --merge --effective-date 2024-03-15     # version the change
uv run python src/scd2.py --show 42                              # see the version history
uv run python src/schema_validate.py --simulate rename           # schema drift → loud FAIL
uv run python src/schema_validate.py --reset                     # restore the contract
uv run python src/data_quality.py 2024-01-15                     # value invariants (shape ≠ values)

# 4b. Edge cases worth feeling once
uv run python src/experiment_upsert_gap.py                       # UPSERT orphans a vanished key
uv run python src/experiment_timezone.py 2024-01-15              # ::date shifts with session TZ

# 5. Hour 3 — the orchestrators (already running from `docker compose up -d`)
#    Airflow  http://localhost:8080   (dev: auto-admin, no login prompt)
#    Dagster  http://localhost:3000
```

`uv run` builds the venv and installs deps (`duckdb`, `psycopg`) on first run.
Scripts default to Postgres on `localhost:5432` (mapped by docker compose) and
write `data/analytics.duckdb`.

> No `uv`? Run the same scripts inside the bundled container:
> `docker compose exec runner python src/<script>.py` (or `./bench python src/<script>.py`).

## Layout

```
src-lesson4/
├── docker-compose.yml      # postgres + runner + airflow(+meta) + dagster
├── Dockerfile              # runner image (python 3.13 + duckdb + psycopg)
├── init.sql                # OLTP source schema (orders + customers)
├── bench                   # optional: docker compose exec runner <cmd>
├── data/                   # analytics.duckdb + staging (gitignored)
├── src/                    # raw-python pipeline (Hours 1-2)
│   ├── config.py           # connection strings + target schema
│   ├── seed_data.py
│   ├── pipeline_naive.py        # Phase 1 — not idempotent
│   ├── pipeline_idempotent.py   # Phase 2 — DELETE+INSERT / UPSERT
│   ├── pipeline_failure.py      # Phase 3 — failure injection + retry
│   ├── pipeline_watermark.py    # Phase 4 — watermark + atomic metadata
│   ├── scd2.py                  # SCD Type 2 dimension merge
│   ├── schema_validate.py       # schema contract check (shape)
│   ├── data_quality.py          # value-level invariants (values)
│   ├── experiment_upsert_gap.py # UPSERT orphaned-key gap vs DELETE+INSERT
│   ├── experiment_timezone.py   # timezone-dependent partition boundary
│   └── prove_idempotent.py      # the take-home proof harness
├── airflow/                # Hour 3 — Airflow DAG (tasks)
│   ├── Dockerfile
│   └── dags/daily_revenue_pipeline.py
└── dagster_app/            # Hour 3 — Dagster assets
    ├── Dockerfile
    ├── assets.py
    └── definitions.py
```

## The four lessons, in code

| Problem | Where | Proof |
|---------|-------|-------|
| Idempotency | `pipeline_idempotent.py` | `prove_idempotent.py` — idempotent vs `--loader naive` |
| Failure recovery | `pipeline_failure.py` | `pipeline_failure.py` — txn rollback + retry |
| Atomic watermark | `pipeline_watermark.py` | data write + metadata write in one txn |
| Slowly Changing Dimensions | `scd2.py` | `scd2.py --show 42` — version history for customer 42 |
| Schema evolution (shape) | `schema_validate.py` | `schema_validate.py --simulate rename` — watch it FAIL |
| Data quality (values) | `data_quality.py` | `data_quality.py 2024-01-15` — value invariants, non-zero exit on failure |

### Edge cases the demos make concrete

| Edge case | Script | What it shows |
|-----------|--------|---------------|
| UPSERT orphaned key | `experiment_upsert_gap.py` | a status that vanishes survives under UPSERT, but not under DELETE+INSERT |
| Timezone partition boundary | `experiment_timezone.py` | `created_at::date` row counts differ by session timezone |

## Idempotency strategy

The target's idempotency comes from **DELETE + INSERT inside one transaction**
(partition replacement), with **UPSERT** (`INSERT OR REPLACE`) as the concise
alternative. The DELETE and the INSERTs must share a transaction: if a crash
happens before COMMIT, the ROLLBACK undoes the DELETE too, so the target is never
left empty. `prove_idempotent.py` runs the pipeline 3× and compares an md5
checksum of the resulting rows.

> **DELETE + INSERT is the safer default.** UPSERT only overwrites keys that
> *reappear*; if a key drops out of a day's data (a status with zero rows), its
> stale row is left orphaned. DELETE + INSERT rewrites the whole partition, so a
> vanished key is removed too. `experiment_upsert_gap.py` demonstrates the
> difference. Prefer UPSERT only when every key is guaranteed to reappear.

## Orchestrators

Both wrap the *same* extract → transform → load:

- **Airflow** (`airflow/dags/daily_revenue_pipeline.py`): tasks + `>>` edges,
  `retries`, `catchup`. UI at :8080 with no login (dev all-admins). Trigger a date
  with config `{"date": "2024-01-15"}`, or backfill a range:
  `docker compose exec airflow airflow backfill create --dag-id daily_revenue_pipeline --from-date 2024-01-10 --to-date 2024-01-20`.
- **Dagster** (`dagster_app/assets.py`): `daily_revenue` is *derived from*
  `raw_daily_orders` (the asset graph IS the lineage). Materialize partitions
  from the UI at `localhost:3000`.

Neither framework gives you idempotency — notice both `load` steps are the same
DELETE + INSERT you wrote by hand. They give you scheduling, retries, lineage,
and observability; correctness is still yours.

> **XCom / IO note:** the lesson warns never to push 1M rows through XCom. Both
> the Airflow and Dagster versions stage the day's orders to a Parquet file and
> pass only the *path* downstream — the "use intermediate storage" advice, made
> concrete.

## Notes & troubleshooting

- **DuckDB is single-writer.** The runner, Airflow, and Dagster share
  `data/analytics.duckdb`. Run one writer at a time, or you'll hit a lock error.
- **Airflow `catchup=False`** by default to avoid backfilling 2 years on a
  laptop. Use the `backfill` command above for a bounded window.
- **First `docker compose up -d` is slow** — it builds the images and pulls Postgres ×2.
- **Reset:** delete `data/analytics.duckdb` (drops the target) or `docker compose down -v`
  (also wipes the seeded source + Airflow metadata).
