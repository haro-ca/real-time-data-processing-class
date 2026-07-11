# Lesson 8 — Stateful stream processing and exactly-once delivery

This is the hardest exercise in the course: a Spark streaming job that joins
transactions with a compacted Kafka topic of customer data, writes to Postgres,
and survives a `kill -9` without duplicating or losing rows.

## Architecture

```
Kafka (transactions) ──┐
                        ├── Spark Structured Streaming ── Postgres
Kafka (customers,      │   stream-static join + upsert      enriched_transactions
 compacted) ────────────┘
```

## Run it

1. Start the infrastructure:
   ```bash
   docker compose up -d
   ```

2. Create the Kafka topics:
   ```bash
   uv run python src/setup_topics.py
   ```

3. Seed the compacted `customers` topic and keep it updating:
   ```bash
   uv run python src/seed_customers.py
   ```

4. In another terminal, start the transaction stream:
   ```bash
   uv run python src/seed_transactions.py --tps 50
   ```

5. Start the Spark streaming job:
   ```bash
   uv run python src/streaming_join.py
   ```

6. After a couple of minutes, check the sink:
   ```bash
   uv run python src/verify.py
   ```

## Kill-and-restart proof

This is the pass/fail test. Run it carefully and record the numbers.

1. Let the pipeline run in steady state for at least 3 minutes.
2. Record the Postgres count:
   ```bash
   uv run python src/verify.py
   ```
3. Hard-kill the Spark driver (`SIGKILL`, not `SIGTERM`).
   ```bash
   ps aux | grep "lesson8-stateful-join" | grep -v grep
   kill -9 <pid>
   ```
4. Keep the transaction generator running. Wait at least 1 minute.
5. Restart Spark with the same `checkpointLocation`:
   ```bash
   uv run python src/streaming_join.py
   ```
6. Let it catch up and run `src/verify.py` again.

The `verify.py` output must report:
- `post_restart_count` > `pre_kill_count` (new transactions were produced).
- `duplicates` = 0 (the upsert sink is idempotent).
- `postgres_count` = `produced` (no data lost, no data duplicated).

## Why it is hard

Spark gives you **exactly-once processing** for the internal state and Kafka
offsets, but the write to Postgres is **at-least-once**. The `ON CONFLICT`
upsert in `src/streaming_join.py` is what makes duplicate writes harmless,
upgrading the whole pipeline to **effectively exactly-once**.

Remove the `ON CONFLICT` and run the kill-and-restart test again: you will see
duplicates corresponding to the partially-written micro-batch at the moment of
the kill.
