# AI assistance context for src-lesson8

## What this code does

`src-lesson8` demonstrates stateful stream processing with end-to-end fault
tolerance. A PySpark Structured Streaming job reads transactions from a Kafka
topic, enriches them with a compacted Kafka topic of customer data, and writes
the results to a Postgres table using `ON CONFLICT ... DO UPDATE` upserts. The
key teaching goal is the kill-and-restart test: after `kill -9` on the Spark
driver, the restarted job must resume without duplicates and without loss.

## Project layout

- `docker-compose.yml` — Kafka (KRaft, single-node) and Postgres.
- `init.sql` — `enriched_transactions` table with primary key on `transaction_id`.
- `src/config.py` — shared constants, SparkSession builder, Kafka/Postgres helpers.
- `src/setup_topics.py` — create `transactions` and compacted `customers` topics.
- `src/seed_customers.py` — seed 1,000 customers and update a few every 5s.
- `src/seed_transactions.py` — steady transaction stream, records `produced` count.
- `src/streaming_join.py` — Spark stream-static join + upsert `foreachBatch` sink.
- `src/verify.py` — count check, duplicate check, and comparison to `produced`.

## Key conventions

- Bootstrap is `localhost:19092` (external Kafka listener from Docker).
- Postgres is `localhost:5432`, user `lesson8`, password `lesson8`, db `lesson8`.
- The `customers` topic is compacted: only the latest value per `customer_id` is
  retained, so it behaves like a mutable table.
- `streaming_join.py` uses a **stream-static join** (the recommended path for
  the exercise). It loads a snapshot of the `customers` topic when the app
  starts, so later customer updates are not visible until the job is restarted.
- The `checkpointLocation` is `ckpt/join`. Deleting it resets the job to `earliest`.

## How to verify correctness

Run the pipeline, then run `verify.py` before and after a `kill -9` on the Spark
driver. The duplicate count must be zero and the Postgres count must match the
number of unique transactions produced.

## Common gotchas

- `kill -15` (SIGTERM) lets Spark finish the current micro-batch cleanly and
  hide the at-least-once behavior. Use `kill -9`.
- Two Spark queries must not share the same `checkpointLocation`.
- `seed_transactions.py` records `data/produced.json` on exit. If it is killed
  with `-9`, the file may not be written; use `verify.py` to query the topic
  end-offsets instead.
- The first Spark run downloads the Kafka and Postgres JDBC JARs from Maven,
  which can take a minute.
