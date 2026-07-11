# Lesson 8 — Stateful stream processing and exactly-once delivery

This is the hardest exercise in the course: Spark streaming jobs that join
transactions with a compacted Kafka topic of customer data, maintain windowed
aggregates in the state store, write to Postgres, and survive a `kill -9`
without duplicating, losing, or miscounting rows.

In the full course pipeline (Lesson 11), the transaction stream comes out of
an OLTP Postgres via Debezium CDC. Here `seed_transactions.py` stands in for
that CDC feed so the lesson can focus on the hard part: fault tolerance.

## Architecture

```
Kafka (transactions) ──┬── streaming_join.py ────── Postgres enriched_transactions
                       │   stream-static join         (idempotent upsert)
Kafka (customers,      │
 compacted) ───────────┘
                       └── streaming_aggregate.py ── Postgres customer_activity
                           windowed aggregation        (upsert on customer_id
                           (real state store)           + window_start)
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

5. Start the Spark streaming join:
   ```bash
   uv run python src/streaming_join.py
   ```

6. In another terminal, start the stateful windowed aggregation:
   ```bash
   uv run python src/streaming_aggregate.py
   ```

7. After a couple of minutes, check both sinks:
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
- aggregate `missing` = 0 and `mismatched` = 0 (state store recovered exactly).

Run the same test against the aggregate job (`lesson8-stateful-aggregate`).
This is the stronger claim: after `kill -9`, the restarted job resumes
mid-window from the recovered state store. If state were lost or replayed
against stale state, the sums in `customer_activity` would be wrong — and no
upsert can repair a wrong sum. `verify.py` proves the sums are exact by
recomputing them from `enriched_transactions` in batch.

## Why it is hard

Spark gives you **exactly-once processing** for the internal state and Kafka
offsets, but the write to Postgres is **at-least-once**. The `ON CONFLICT`
upsert in `src/streaming_join.py` is what makes duplicate writes harmless,
upgrading the whole pipeline to **effectively exactly-once**.

Remove the `ON CONFLICT` and run the kill-and-restart test again: you will see
duplicates corresponding to the partially-written micro-batch at the moment of
the kill.

The two jobs fail differently, and that is the point:

| Job | What protects it | How it fails without protection |
|---|---|---|
| `streaming_join.py` | idempotent sink (`ON CONFLICT`) | duplicate rows |
| `streaming_aggregate.py` | state store + checkpoint | wrong sums (silent!) |
