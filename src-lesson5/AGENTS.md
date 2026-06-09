# AGENTS.md — Lesson 5 Change Data Capture

Context for AI coding assistants (opencode, Claude, Cascade) working in this repo.

## What this is

A teaching repo for **log-based Change Data Capture**: reading the Postgres WAL
through a logical replication slot and maintaining a live DuckDB mirror, by hand,
then comparing it to Debezium. Follows Lesson 4 (same `bench` source, same DuckDB
target idea) — the bridge changes from nightly batch to a continuous stream.

## Architecture

- **Source:** Postgres 16 + `wal2json` (`./postgres/Dockerfile`), `bench`/`bench`@`bench`,
  table `orders` (+ `customers`), `wal_level=logical` set in compose.
- **Target:** single DuckDB file `data/cdc.duckdb`. Mirror schema + `mirror_checksum`
  live in `src/config.py`.
- **CDC wiring:** `setup_cdc.py` creates `REPLICA IDENTITY FULL`, publication
  `orders_pub`, and slot `orders_slot` (plugin `wal2json`).
- **Consumer:** `cdc_consumer.py` = peek → apply (delete+insert) → advance.

## Connection contract (env, with localhost defaults)

- `PG_HOST` (default `localhost`; `postgres` inside compose), `PG_PORT` (5432)
- `DUCKDB_PATH` (default `<repo>/data/cdc.duckdb`)
- `CDC_SLOT` (default `orders_slot`)
- DSNs derived in `src/config.py` — import from there, don't hardcode.

## How to run

```bash
docker compose up -d
uv run python src/seed_data.py            # seed BEFORE creating the slot
uv run python src/setup_cdc.py
uv run python src/snapshot.py
uv run python src/cdc_consumer.py
# no uv? docker compose exec runner python src/<script>.py
```

## Hard constraints (do not break)

- **Seed before the slot.** The slot only carries post-`consistent_point` changes;
  the snapshot covers the rest. This ordering is the whole "initial snapshot" lesson.
- **Apply must be idempotent.** UPDATE/DELETE+INSERT keyed on `id`, each event in one
  DuckDB transaction. CDC is at-least-once; replay must be a no-op. Do not switch to
  blind `UPDATE ... SET` or non-transactional applies.
- **Confirm AFTER applying.** Peek (no advance) → apply → `pg_replication_slot_advance`.
  Never advance/confirm before the DuckDB write is durable, or a crash loses data.
- **DuckDB is single-writer.** Never run two writers against `data/cdc.duckdb` at once
  (consumer vs poll_sync vs snapshot). Run one at a time.
- **wal2json, not pgoutput.** We read JSON on purpose (concepts over byte-parsing).
  Debezium (overlay) uses `pgoutput`; that's the contrast, keep it.
- **Every slot needs a consumer.** Don't leave slots around; an unconsumed slot
  retains WAL until the disk fills (`experiment_abandon_slot.py` demonstrates it).
  `max_slot_wal_keep_size` is intentionally unset so the growth is visible.

## Style

- Match Lesson 4: `argparse` CLIs, `if __name__ == "__main__"`, env-overridable
  config in `config.py`, short module docstrings with a Usage block.
- Python 3.13. Deps: `duckdb`, `psycopg[binary]` (v3). Don't add heavy deps.
- Bulk copies (seed/snapshot/poll) use the DuckDB postgres extension; CDC control
  (slot, peek, advance, lag) uses `psycopg`.

## Gotchas

- `wal2json` is NOT in stock `postgres:16` — `./postgres/Dockerfile` installs it.
- wal2json ignores publications; the consumer filters with the `add-tables` option.
  Publications still matter for the Debezium/`pgoutput` path.
- Replication slot functions want autocommit connections (`psycopg.connect(..., autocommit=True)`).
- Debezium overlay (`debezium/`) is optional show-and-tell; it creates its OWN slot
  (`debezium_slot`) — drop it on teardown so it stops retaining WAL.
