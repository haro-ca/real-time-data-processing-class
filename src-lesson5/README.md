# Lesson 5 — Change Data Capture (Postgres → DuckDB)

Stop polling the database for "what changed?" and **subscribe** to its change
stream instead. We build a log-based CDC consumer by hand — reading the Postgres
WAL through a `wal2json` replication slot and maintaining a live DuckDB mirror —
then break it the four ways production breaks it, and finish by watching Debezium
automate the whole thing.

Same source and target as Lesson 4 (`bench` Postgres → DuckDB); only the bridge
changes: a continuous stream instead of a nightly batch.

## Stack

- **Source:** Postgres 16 + `wal2json`, `wal_level=logical` (`./postgres`, `init.sql`).
- **Target:** a DuckDB mirror at `data/cdc.duckdb` (schema in `src/config.py`).
- **Runner:** `runner` container with `duckdb` + `psycopg[binary]`, scripts in `src/`.
- **Optional:** Debezium overlay in `debezium/` (the production-tool show-and-tell).

## Quickstart

```bash
docker compose up -d                          # Postgres (wal2json) + runner
uv run python src/seed_data.py                # 1M orders BEFORE the slot exists

# 1) Why polling is the wrong question — watch the copy silently drift
uv run python src/poll_sync.py --audit

# 2) Build the log-based consumer
uv run python src/setup_cdc.py                # REPLICA IDENTITY + publication + slot
uv run python src/snapshot.py                 # backfill the pre-slot million
uv run python src/cdc_consumer.py &           # stream every change into DuckDB

# 3) Operate it
uv run python src/watch_lag.py                # the one metric that matters

# 4) Break it
uv run python src/cdc_consumer.py --crash-after 20   # replay is harmless (idempotent)
uv run python src/experiment_abandon_slot.py         # a forgotten slot fills the disk
```

No `uv` on the host? Run inside the container:
`docker compose exec runner python src/<script>.py <args>`.

## The four demos that matter

| Step | Script | Shows |
|------|--------|-------|
| Polling lies | `poll_sync.py --audit` | deletes invisible, missed `updated_at` → silent drift |
| Build it | `setup_cdc.py` → `snapshot.py` → `cdc_consumer.py` | slot, snapshot+stream, idempotent apply, LSN confirm |
| Keep up | `watch_lag.py` | `lag_bytes` = current WAL − confirmed LSN |
| Crash & replay | `cdc_consumer.py --crash-after N` | at-least-once + idempotency = same checksum |
| Disk-full | `experiment_abandon_slot.py` | unconsumed slot retains WAL without bound |

## How the consumer works (and how it maps to the slides)

`cdc_consumer.py` loops: **peek** the slot (`pg_logical_slot_peek_changes`, which
does *not* advance) → **apply** each change to DuckDB with `DELETE + INSERT` on the
primary key (idempotent) → **advance** the slot (`pg_replication_slot_advance`) =
"confirm". Crash between apply and advance and the next run re-applies the same
changes harmlessly. The slides show this as a streaming `send_feedback(lsn)` loop;
peek+advance teaches the same *apply-then-confirm* discipline with plain SQL and no
replication-protocol plumbing. Debezium uses the streaming protocol with `pgoutput`.

## Reset

```bash
docker compose exec postgres psql -U bench -d bench \
  -c "SELECT pg_drop_replication_slot('orders_slot');"   # then re-run setup_cdc.py
rm -f data/cdc.duckdb                                     # clean mirror
docker compose down -v                                    # wipe source + slots entirely
```
