# Lesson 8, Stateful stream processing and exactly-once delivery

This is the hardest exercise in the course. Not because the concepts are novel, students already know Kafka, already know Spark Structured Streaming from Lesson 7, already know Postgres. The difficulty is that they must make all of these systems work together under failure, and prove it. Every shortcut they've taken in previous lessons, ignoring duplicate delivery, not thinking about what happens on restart, assuming the happy path, comes due here.

## Hour 1, Theory: state, joins, and the impossibility of exactly-once

### Module A, Why state changes everything

In Lesson 7, every operation was stateless. A `map` or `filter` takes an event, produces an output, forgets the event. If the processor crashes and replays the input, you get the same output. Idempotency is free.

Now consider a `COUNT`. The processor must remember how many events it has seen. That memory is state. If the processor crashes after counting 1000 events but before writing the count to the sink, what happens on restart? If it replays from event 1, the count resets to zero and rebuilds, correct but slow. If it replays from event 950 (the last committed offset), it counts 50 events and gets 50 instead of 1000. Wrong.

State makes failure recovery non-trivial because you now have two things to keep in sync: the position in the input stream (offset) and the accumulated computation (state). If they diverge, you get wrong answers. This is the central problem of this lesson.

### Module B, Stateful operations: joins, sessions, deduplication

**Stream-table joins (the practical exercise).** You have a continuous stream of transactions and a slowly-changing table of customer data. For each transaction, look up the customer and attach their metadata (name, tier, region). The "table" side is a compacted Kafka topic, Kafka retains only the latest value per key, so it behaves like a mutable table. The stream processing engine materializes this topic into local state (a key-value store) and probes it for every arriving transaction.

This is conceptually simple but operationally subtle. What happens when a customer record updates *after* some of their transactions have already been enriched with the old data? The answer depends on whether you need point-in-time correctness or eventual correctness. For this exercise, eventual correctness is fine, we're enriching with the latest customer data at processing time.

**Stream-stream joins.** Two event streams that must be correlated, e.g., ad impressions and ad clicks, joined on a click ID within a time window. Both sides are unbounded, so the engine must buffer events from one stream while waiting for matching events from the other. The buffer is bounded by the join window. A 1-hour join window on a stream with 100k events/second means buffering up to 360M events. This is where state backends (next module) become critical.

Students don't implement a stream-stream join in the practical, but they need to understand why it's harder than a stream-table join: both sides are in motion, the state is proportional to the window size times the event rate, and late arrivals on either side can invalidate or update previously-emitted join results.

**Sessionization.** Group events by a key (user ID) into sessions defined by an inactivity gap. If no event arrives for a user within 30 minutes, the session closes. This requires per-key timers and state, the engine must remember the last event time for every active user and fire a callback when the gap expires. Session windows are variable-length, which makes them harder to checkpoint than fixed windows.

**Deduplication.** The stream contains duplicate events (a producer retried, a CDC connector replayed). The processor must emit each event exactly once. This requires remembering every event ID it has seen, a potentially unbounded set. In practice, you bound it with a time window: "deduplicate within the last 10 minutes." The state is a set of event IDs per window. After the window expires, the IDs are evicted. If a duplicate arrives after eviction, it passes through. This is a tradeoff, memory vs. deduplication guarantee window.

### Module C, State backends: where does the state live?

In Flink, the default state backend is heap-based (JVM memory). Fast, but limited by RAM. For large state (millions of keys), Flink uses RocksDB, an embedded LSM-tree key-value store. State spills to local disk, with the hot set in memory. This lets Flink manage state far exceeding available RAM.

Spark Structured Streaming takes a different approach. State is managed by the `StateStore` abstraction, backed by HDFS-compatible storage (or local disk in standalone mode). On each micro-batch, Spark:

1. Reads the previous state from the state store.
2. Processes the new micro-batch, updating state as needed.
3. Writes the updated state back.
4. Commits the offset.

Steps 3 and 4 together form the atomicity boundary. If the driver crashes between writing state and committing the offset, on restart it replays the micro-batch, but the state was already written. Spark handles this by versioning state: each micro-batch produces a new state version, and the commit log records which version corresponds to which offset. On recovery, Spark rolls back to the last committed version.

For the practical: students use Spark Structured Streaming with the default file-based state store. They don't need to think about RocksDB, but they need to understand that state management is what makes checkpointing possible.

### Module D, Checkpointing and savepoints

**Checkpointing** is the mechanism that makes fault tolerance possible for stateful processors. The idea: periodically snapshot the processor's state and input position (offsets) in a consistent way, so that on failure you can restore to the snapshot and replay from the recorded offsets.

In Spark Structured Streaming, checkpointing works as follows:

1. The driver writes checkpoint metadata to a reliable storage location (`checkpointLocation`).
2. For each micro-batch, the checkpoint records: (a) the Kafka offsets that define the batch boundaries, (b) the state store version after processing the batch, and (c) the committed output (for sinks that support it).
3. On restart, Spark reads the checkpoint, determines the last successfully completed micro-batch, restores state to that version, and resumes from the next offset.

This gives you **exactly-once processing semantics within Spark**, the combination of replayable source (Kafka) + checkpointed state + deterministic processing means that after recovery, the internal state is identical to what it would have been without the failure.

But here's the critical gap: **exactly-once processing does not mean exactly-once delivery to the sink.** Spark processes the micro-batch, updates state, writes output to the sink, and commits the checkpoint. If it crashes after writing to the sink but before committing the checkpoint, on restart it replays the micro-batch and writes to the sink *again*. The sink now has duplicates. Spark guarantees at-least-once delivery to external sinks. Making it exactly-once is the student's job.

**Savepoints** (a Flink concept, but worth mentioning) are user-triggered checkpoints. Use case: you want to stop the job, upgrade the code, and restart from exactly where you left off. Savepoints are also how you migrate state between job versions. Spark doesn't have a direct equivalent, but the checkpoint directory serves a similar purpose for restarts with the same application.

### Module E, Exactly-once end-to-end: the real problem

The end-to-end exactly-once guarantee requires all three components to cooperate:

1. **Source (Kafka):** must be replayable, given an offset, it can re-deliver the same messages. Kafka provides this naturally.
2. **Processor (Spark):** must checkpoint state and offsets atomically. Spark provides this via the checkpointing mechanism.
3. **Sink (Postgres):** must handle duplicate writes without creating duplicate data. **Spark does not provide this.** This is the student's problem.

The standard approach: **make the sink idempotent.** Two strategies:

**Strategy 1, Upsert on a unique key.** Design the output table with a natural unique key (e.g., `transaction_id`). Use `INSERT ... ON CONFLICT (transaction_id) DO UPDATE SET ...` (Postgres) or `REPLACE INTO` (MySQL). If the same record is written twice, the second write overwrites the first. The result is the same as if it was written once.

**Strategy 2, Offset tracking table.** Maintain a separate table that records the last successfully committed offset (or micro-batch ID). Wrap the sink write and the offset update in a single database transaction. On restart, before writing, check the offset table, if the micro-batch was already committed, skip it. This is more complex but works for sinks where upsert isn't natural.

For this lesson, students implement Strategy 1 because it's simpler and more broadly applicable. But they should understand why Strategy 2 exists: some outputs (aggregations, for example) don't have a natural unique key from the source, and upsert semantics may not capture the correct computation.

### Module F, The two generals problem, practically

The two generals problem proves that no protocol can guarantee agreement between two parties over an unreliable channel in a finite number of messages. In practical terms: there is no way to atomically commit a Spark micro-batch **and** a Postgres transaction if the two systems cannot participate in a shared transaction protocol.

This is why true end-to-end exactly-once is impossible in the general case between independent systems. What we actually achieve is **effectively exactly-once**, at-least-once delivery combined with idempotent sinks. The duplicates happen, but they don't affect the final result.

Students who claim "Kafka provides exactly-once" should be challenged: Kafka's exactly-once semantics (idempotent producer + transactional consumer + transactional producer to output topic) work because all three components are inside Kafka's transaction boundary. The moment your sink is Postgres, you've crossed a transaction boundary, and the two generals problem applies.

This is not academic pedantry. It determines how you design the sink. If you don't internalize this, you'll build pipelines that lose or duplicate data under failure, and you won't understand why.

---

## Hour 2, Practical: the stateful join pipeline

This exercise has more moving parts than anything students have built so far. The architecture:

```
Kafka (transactions topic) ──┐
                              ├── Spark Structured Streaming ── Postgres (enriched_transactions)
Kafka (customers topic) ─────┘
        (compacted)            (stream-table join + checkpoint)    (idempotent sink via upsert)
```

### Setup (20 min)

Students should already have Kafka running from Lesson 6. They need:

1. **Kafka topics:**

```bash
# Transactions: regular topic, 4 partitions
kafka-topics.sh --create --topic transactions \
    --partitions 4 --replication-factor 1 \
    --bootstrap-server localhost:9092

# Customers: compacted topic, Kafka retains only the latest value per key
kafka-topics.sh --create --topic customers \
    --partitions 4 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.01 \
    --config segment.ms=100 \
    --bootstrap-server localhost:9092
```

The compacted topic is critical. Explain what compaction does: Kafka's log cleaner periodically scans closed segments and removes older records with the same key, keeping only the latest. This turns a Kafka topic into a pseudo-table, you can consume it from the beginning to reconstruct the full current state.

2. **Postgres sink table:**

```sql
CREATE TABLE enriched_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_time TIMESTAMPTZ NOT NULL,
    -- Enrichment fields from customer data
    customer_name VARCHAR(256),
    customer_tier VARCHAR(32),
    customer_region VARCHAR(64),
    -- Metadata
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Note the `PRIMARY KEY` on `transaction_id`. This is what makes the upsert strategy possible. Without it, there is no idempotency.

3. **Data generators.** Provide two python scripts:

**Customer data generator**, seeds the compacted topic with ~1000 customers, then periodically updates a few (simulating tier changes, address updates):

```python
import json
import time
import random
import string
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]

def generate_customers(n=1000):
    customers = []
    for i in range(n):
        customer_id = f"cust-{i:06d}"
        customer = {
            "customer_id": customer_id,
            "name": f"Customer {i}",
            "tier": random.choice(TIERS),
            "region": random.choice(REGIONS),
        }
        customers.append(customer)
        # Key MUST be the customer_id for compaction to work correctly
        producer.produce(
            "customers",
            key=customer_id.encode("utf-8"),
            value=json.dumps(customer).encode("utf-8"),
        )
        if i % 100 == 0:
            producer.flush()
    producer.flush()
    return customers

def update_customers_periodically(customers, interval=5.0):
    """Randomly update a few customers every interval."""
    while True:
        batch = random.sample(customers, k=min(5, len(customers)))
        for c in batch:
            c["tier"] = random.choice(TIERS)
            producer.produce(
                "customers",
                key=c["customer_id"].encode("utf-8"),
                value=json.dumps(c).encode("utf-8"),
            )
        producer.flush()
        time.sleep(interval)

if __name__ == "__main__":
    customers = generate_customers()
    print(f"Seeded {len(customers)} customers")
    update_customers_periodically(customers)
```

**Transaction generator**, produces a steady stream of transactions referencing existing customer IDs:

```python
import json
import time
import uuid
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

CURRENCIES = ["USD", "EUR", "GBP"]

def generate_transactions(customer_count=1000, tps=50):
    interval = 1.0 / tps
    while True:
        txn = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"cust-{random.randint(0, customer_count - 1):06d}",
            "amount": round(random.uniform(1.0, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
            "transaction_time": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            "transactions",
            key=txn["customer_id"].encode("utf-8"),
            value=json.dumps(txn).encode("utf-8"),
        )
        producer.poll(0)
        time.sleep(interval)

if __name__ == "__main__":
    generate_transactions()
```

Students should start both generators and verify data is flowing: `kafka-console-consumer --topic transactions --bootstrap-server localhost:9092 --max-messages 5` should show JSON transaction events.

### Phase 1, The stream-table join in Spark (25 min)

This is the core of the exercise. Students build a PySpark Structured Streaming application that:

1. Reads the `transactions` topic as a streaming DataFrame.
2. Reads the `customers` topic as a streaming DataFrame (which Spark will materialize as state for the join).
3. Joins them on `customer_id`.
4. Writes the enriched result to Postgres.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("lesson8-stateful-join")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/lesson8/checkpoint")
    .getOrCreate()
)

# --- Schema definitions ---
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_time", StringType()),
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("tier", StringType()),
    StructField("region", StringType()),
])

# --- Read transactions as a stream ---
transactions = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), transaction_schema).alias("data"))
    .select("data.*")
)

# --- Read customers as a stream (will be used in stream-stream join) ---
customers = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customers")
    .option("startingOffsets", "earliest")
    .load()
    .select(
        from_json(col("value").cast("string"), customer_schema).alias("data"),
        col("timestamp").alias("customer_update_time"),
    )
    .select("data.*", "customer_update_time")
    .withWatermark("customer_update_time", "1 hour")
)
```

Now the join. There's a decision here that students must make and defend. Spark Structured Streaming supports stream-stream joins but requires watermarks on both sides to bound the state. For a stream-table join pattern (where the "table" side is a compacted topic representing current state), the cleanest approach in Spark is to read the customer topic as a **batch DataFrame** and use a stream-static join:

```python
# --- Alternative: stream-static join ---
# Read the compacted customers topic as a batch (current snapshot)
customers_static = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customers")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), customer_schema).alias("data"))
    .select("data.*")
)

# Deduplicate: keep only the latest record per customer_id
# (compaction may not have run yet, so duplicates may exist)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

customers_deduped = (
    customers_static
    .withColumn("_row", monotonically_increasing_id())
    .withColumn(
        "_rank",
        row_number().over(
            Window.partitionBy("customer_id").orderBy(col("_row").desc())
        ),
    )
    .filter(col("_rank") == 1)
    .drop("_row", "_rank")
)

# Stream-static join: each micro-batch of transactions joins against
# the static customer snapshot
enriched = transactions.join(customers_deduped, on="customer_id", how="left")
```

**The tradeoff students must articulate:** a stream-static join uses the customer data as it was when the Spark application started. If customer data changes while the pipeline is running, the enrichment will use stale data. For this exercise, that's acceptable, customer data changes slowly, and the pipeline can be restarted periodically. In production, you'd either (a) use a stream-stream join with watermarks, (b) periodically refresh the static DataFrame, or (c) use Flink's native table support which handles this more elegantly.

Students who want to attempt the stream-stream join variant should be encouraged but warned: they'll need watermarks on both sides, and the state management gets significantly more complex. The stream-static join is the recommended path for completing the exercise in the allotted time.

### Phase 2, The idempotent Postgres sink (20 min)

This is where the lesson's thesis becomes code. Spark's built-in JDBC sink (`format("jdbc")`) appends rows. If a micro-batch is replayed after failure, you get duplicates. Students must write a custom `foreachBatch` sink that performs upserts:

```python
def write_to_postgres(batch_df, batch_id):
    """
    Write a micro-batch to Postgres using upserts.
    This makes the sink idempotent: replaying the same batch
    produces the same result as writing it once.
    """
    if batch_df.isEmpty():
        return

    # Collect to driver, acceptable for moderate throughput.
    # For high throughput, use JDBC batch writes from executors.
    rows = batch_df.collect()

    import psycopg
    conn = psycopg.connect("postgresql://user:password@localhost:5432/lesson8")

    upsert_sql = """
        INSERT INTO enriched_transactions
            (transaction_id, customer_id, amount, currency,
             transaction_time, customer_name, customer_tier, customer_region)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            customer_tier = EXCLUDED.customer_tier,
            customer_region = EXCLUDED.customer_region,
            processed_at = now()
    """

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(upsert_sql, (
                row["transaction_id"],
                row["customer_id"],
                row["amount"],
                row["currency"],
                row["transaction_time"],
                row["customer_name"],
                row["tier"],
                row["region"],
            ))
    conn.commit()
    conn.close()


# --- Wire it up ---
query = (
    enriched.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/lesson8/checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
```

**Key points to cover during this phase:**

1. **Why `foreachBatch` and not `foreach`?** `foreachBatch` gives you access to the entire micro-batch as a DataFrame, so you can do efficient batch writes. `foreach` processes one row at a time, slow and hard to make transactional.

2. **Why `ON CONFLICT DO UPDATE` and not `ON CONFLICT DO NOTHING`?** `DO NOTHING` silently discards the duplicate, which is fine if the data is identical. But if customer data has been updated between the original write and the replay, `DO UPDATE` ensures the enrichment reflects the latest data. Think about which is correct for your use case.

3. **The `collect()` problem.** Calling `collect()` pulls all data to the driver. For this exercise at 50 TPS with 10-second micro-batches (~500 rows), it's fine. In production, you'd use the JDBC connector from executors, or write a custom sink that distributes the upsert work across the cluster. Students should note this as a limitation in their README.

4. **Connection management.** The example opens and closes a connection per micro-batch. In production, use a connection pool. But for the exercise, correctness matters more than efficiency.

### Phase 3, Verify the happy path (10 min)

Before they break anything, students must establish the baseline:

1. Start the customer generator and let it seed 1000 customers.
2. Start the transaction generator at 50 TPS.
3. Start the Spark streaming job.
4. Let it run for 2 minutes.
5. Count the rows in Postgres: `SELECT COUNT(*) FROM enriched_transactions;`
6. Count the messages in Kafka: use `kafka-consumer-groups.sh --describe --group <spark-consumer-group>` to see the current offset.
7. Verify the counts are consistent: Postgres rows should roughly equal the total Kafka messages consumed (exact match if no failures).

Also run a spot-check:

```sql
SELECT t.transaction_id, t.customer_name, t.customer_tier
FROM enriched_transactions t
ORDER BY t.processed_at DESC
LIMIT 10;
```

Every row should have customer enrichment data (non-null `customer_name`, `customer_tier`, `customer_region`). If any are null, the join didn't match, debug now, not after you start killing processes.

---

## Hour 3, Break it, fix it, prove it

This is the centerpiece of the lesson and of the course. The exercise is pass/fail: either the student can demonstrate exactly-once behavior across a processor failure, or they can't.

### The kill-and-restart protocol (40 min)

This protocol must be followed precisely. Sloppy methodology produces ambiguous results.

**Step 1, Establish pre-kill state.**

Let the pipeline run in steady state for at least 3 minutes. Then:

```sql
-- Record the count before the kill
SELECT COUNT(*) AS pre_kill_count FROM enriched_transactions;
-- Also record the max transaction time to establish a temporal boundary
SELECT MAX(transaction_time) AS last_processed FROM enriched_transactions;
```

Write these numbers down. They're the baseline.

Also pause the transaction generator momentarily and note the total number of messages produced to the `transactions` topic:

```bash
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic transactions \
    --time -1
```

Sum the offsets across partitions. Call this `kafka_offset_at_kill`.

**Step 2, Kill the processor.**

Not a graceful shutdown. A hard kill. This simulates the worst case, the processor crashes between writing to Postgres and committing the checkpoint.

```bash
# Find the Spark driver process
ps aux | grep "lesson8-stateful-join" | grep -v grep

# Kill it hard, SIGKILL, not SIGTERM
kill -9 <pid>
```

`SIGTERM` triggers a graceful shutdown where Spark finishes the current micro-batch and commits the checkpoint. That's too easy. `SIGKILL` forces an unclean shutdown mid-micro-batch, which is the scenario that creates duplicates if the sink isn't idempotent.

**Step 3, Keep producing.**

The transaction generator keeps running. New transactions are accumulating in Kafka while the processor is dead. This is realistic, in production, you don't stop the world when a processor crashes.

Let at least 1 minute pass. New transactions pile up.

**Step 4, Record the gap.**

```bash
# Check current Kafka offsets, more messages have arrived since the kill
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic transactions \
    --time -1
```

Call this `kafka_offset_at_restart`. The difference `kafka_offset_at_restart - kafka_offset_at_kill` tells you how many messages accumulated during the outage.

**Step 5, Restart the processor.**

Start the Spark job again with the same checkpoint location. Spark should:

1. Read the checkpoint.
2. Determine the last committed micro-batch.
3. Resume consuming from the next offset.
4. Process the backlog.
5. Catch up to the live stream.

Watch the logs. Students should see Spark report something like "Resuming from checkpoint" and then process multiple micro-batches rapidly as it catches up.

**Step 6, Wait for catch-up and verify.**

Once the consumer group lag drops to near zero (check with `kafka-consumer-groups.sh`), stop the transaction generator and let the pipeline drain.

Now the moment of truth:

```sql
-- Count after restart and catch-up
SELECT COUNT(*) AS post_restart_count FROM enriched_transactions;
```

**What this number must be:**

- `post_restart_count` should equal the total number of unique transactions produced to Kafka.
- It must be **greater than** `pre_kill_count` (new transactions were produced during the outage).
- It must **not** exceed the total unique transaction count (no duplicates).

Verify there are no duplicates:

```sql
-- This MUST return zero rows
SELECT transaction_id, COUNT(*) AS dupes
FROM enriched_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

If this returns any rows, the student's idempotency implementation is broken.

Also verify no data was lost:

```sql
-- Compare total unique transactions produced vs. rows in Postgres
-- The student should have this count from their generator or from Kafka offsets
SELECT COUNT(*) FROM enriched_transactions;
```

This count should match the total number of unique `transaction_id` values produced. If it's less, data was lost, the checkpoint didn't cover some processed-but-unwritten transactions, or the restart skipped some offsets.

### The failure mode students will actually hit (15 min discussion)

After the exercise, gather the class. Some students will have clean results. Others will find duplicates or missing data. Walk through the common failure modes:

**Failure mode 1, Duplicates (most common).** The student used `format("jdbc").mode("append")` instead of the `foreachBatch` upsert. On replay, duplicate rows appear. Fix: implement the upsert sink from Phase 2.

**Failure mode 2, Missing enrichment data.** The stream-static join loaded customer data at startup. After the kill, the restarted job re-read the customers topic, but if the compacted topic hadn't fully compacted yet, some customer records might have been lost or the deduplicated snapshot might differ slightly. Fix: ensure the customer deduplication logic is deterministic, or use a database lookup instead of a Kafka-sourced static join.

**Failure mode 3, Checkpoint corruption.** If a student used a local filesystem checkpoint and the disk was shared with the Spark shuffle space, a crash can corrupt the checkpoint directory. Spark fails to restart and throws `StreamingQueryException: Failed to read checkpoint`. Fix: use a separate, reliable storage location. In production, this would be HDFS or S3. For the exercise, a dedicated local directory that isn't on a tmpfs is sufficient.

**Failure mode 4, The student used `kill -15` instead of `kill -9`.** Graceful shutdown means Spark committed the checkpoint cleanly. The restart works perfectly, but it didn't test the hard failure scenario. This is cheating (even if unintentional). Re-run with `kill -9`.

### Experiment, What happens without the upsert? (15 min)

For students who got the exercise working: remove the `ON CONFLICT` clause. Replace it with a plain `INSERT`. Truncate the table. Re-run the entire kill-and-restart protocol.

```sql
-- After the restart, check for duplicates
SELECT transaction_id, COUNT(*) AS dupes
FROM enriched_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY dupes DESC
LIMIT 20;
```

There will be duplicates. The number of duplicates corresponds roughly to the size of the last in-progress micro-batch at the time of the kill. This makes the at-least-once behavior visible and quantifiable.

Ask: **how many duplicates did you get, and why that number?**

The answer should reference the trigger interval (10 seconds) and the TPS (50): at most ~500 duplicate rows, corresponding to one micro-batch that was partially written to Postgres but not committed in the checkpoint. The exact number depends on where in the micro-batch the `kill -9` landed.

### Final synthesis (10 min)

Put this diagram on the board and have students fill in the guarantees at each boundary:

```
Kafka ──[exactly-once replay]──> Spark ──[at-least-once write]──> Postgres
         (replayable source)      (checkpointed state)           (idempotent upsert)
                                                                       │
                                                          makes at-least-once
                                                          effectively exactly-once
```

The end-to-end guarantee is only as strong as the weakest link. Kafka provides replayability. Spark provides checkpointed state. But the Spark-to-Postgres boundary is at-least-once by default. The upsert makes duplicate writes harmless, upgrading the effective guarantee to exactly-once.

Ask the closing question: **could you achieve true exactly-once (not "effectively exactly-once") between Spark and Postgres?**

Answer: only if Spark and Postgres participated in a distributed transaction (2PC), Spark would prepare the write, Postgres would prepare the write, and both would commit atomically. This is theoretically possible (XA transactions exist), but practically nobody does it because: (a) it's slow (2PC adds latency), (b) it's fragile (coordinator failure blocks both systems, as discussed in Lesson 2), and (c) idempotent sinks are simpler and more robust. The two generals problem tells you that no finite protocol can guarantee agreement between Spark and Postgres if the network between them can fail. Idempotency sidesteps the problem entirely by making duplicate delivery harmless.

---

## Take-home deliverable

A repository submitted via pull request, containing:

1. **The streaming pipeline code**, complete, runnable PySpark application that reads transactions from Kafka, joins with customer data, and writes enriched results to Postgres via idempotent upsert.

2. **Data generator scripts**, the customer and transaction generators (modified or as provided).

3. **Infrastructure setup**, Docker Compose or equivalent for Kafka, Postgres, and Spark. Another student should be able to `docker compose up` and run the pipeline.

4. **README.md**, must include:
   - Architecture diagram (text-based is fine)
   - How to run the pipeline
   - The kill-and-restart protocol they followed
   - **The proof:** pre-kill count, post-restart count, the duplicate-check query returning zero rows, and the total count matching the expected number of unique transactions. Screenshots or copy-pasted query output are acceptable. Fabricated results are not, the numbers must be internally consistent (pre-kill count < post-restart count, post-restart count = total produced, zero duplicates).
   - Discussion of what happens without the upsert (how many duplicates, why)

5. **CLAUDE.md**, instructions for an AI coding assistant to understand, run, and modify the pipeline. This should include the project structure, key design decisions (why upsert, why stream-static join, why `foreachBatch`), and common failure modes.

This is the hardest deliverable in the course because correctness under failure is harder to demonstrate than performance. A pipeline that produces correct output on the happy path is not sufficient. The deliverable must include evidence of correct behavior across a hard failure. No proof, no pass.
