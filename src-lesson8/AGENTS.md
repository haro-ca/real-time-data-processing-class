# AI assistance context for src-lesson8

## What this code does

`src-lesson8` demonstrates stateful stream processing with end-to-end fault
tolerance. Two PySpark Structured Streaming jobs read transactions from Kafka:
`streaming_join.py` enriches them with a compacted customer topic and upserts
into Postgres; `streaming_aggregate.py` maintains per-customer per-minute
windowed aggregates in the Spark state store and upserts them into
`customer_activity`. The key teaching goal is the kill-and-restart test: after
`kill -9` on a Spark driver, the restarted job must resume without duplicates,
without loss, and with exact aggregate sums (proving state store recovery).

## Project layout

- `docker-compose.yml` — Kafka (KRaft, single-node) and Postgres.
- `init.sql` — `enriched_transactions` (PK `transaction_id`) and
  `customer_activity` (PK `customer_id, window_start`) tables.
- `src/config.py` — shared constants, SparkSession builder, Kafka/Postgres helpers.
- `src/setup_topics.py` — create `transactions` and compacted `customers` topics.
- `src/seed_customers.py` — seed 1,000 customers and update a few every 5s.
- `src/seed_transactions.py` — steady transaction stream, records `produced` count.
- `src/streaming_join.py` — Spark stream-static join + upsert `foreachBatch` sink.
- `src/streaming_aggregate.py` — genuinely stateful windowed aggregation
  (watermark + update mode) upserting into `customer_activity`.
- `src/verify.py` — count check, duplicate check, comparison to `produced`, and
  batch recomputation of the streaming aggregates.

## Key conventions

- Bootstrap is `localhost:19092` (external Kafka listener from Docker).
- Postgres is `localhost:5432`, user `lesson8`, password `lesson8`, db `lesson8`.
- The `customers` topic is compacted: only the latest value per `customer_id` is
  retained, so it behaves like a mutable table.
- `streaming_join.py` uses a **stream-static join** (the recommended path for
  the exercise). It loads a snapshot of the `customers` topic when the app
  starts, so later customer updates are not visible until the job is restarted.
- `streaming_aggregate.py` is the truly stateful job: sums live in the state
  store between micro-batches, and `amount` is cast to `decimal(12,2)` before
  summing so streaming sums match Postgres batch recomputation exactly.
- Checkpoints: `ckpt/join` and `ckpt/aggregate` (one per query, never shared).
  Deleting a checkpoint resets that job to `earliest`.

## How to verify correctness

Run the pipeline, then run `verify.py` before and after a `kill -9` on the Spark
driver. The duplicate count must be zero, the Postgres count must match the
number of unique transactions produced, and the streaming aggregates must match
the batch recomputation (0 missing, 0 mismatched windows).

## Common gotchas

- `kill -15` (SIGTERM) lets Spark finish the current micro-batch cleanly and
  hide the at-least-once behavior. Use `kill -9`.
- Two Spark queries must not share the same `checkpointLocation`.
- `seed_transactions.py` records `data/produced.json` on exit. If it is killed
  with `-9`, the file may not be written; use `verify.py` to query the topic
  end-offsets instead.
- The first Spark run downloads the Kafka and Postgres JDBC JARs from Maven,
  which can take a minute.
- `init.sql` only runs on a fresh Postgres volume. `streaming_aggregate.py`
  creates `customer_activity` itself if it is missing, so existing volumes work.
- `verify.py` only compares aggregate windows that closed 2+ minutes ago, so
  run it a couple of minutes after the last transaction lands.
