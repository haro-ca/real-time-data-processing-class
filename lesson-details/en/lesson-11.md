# Lesson 11, End-to-end pipeline: OLTP → CDC → Kafka → Stream Processing → Real-time OLAP → API

Every previous lesson isolated one component and studied it under controlled conditions. That's not how production works. In production, these components are wired together, and the failure modes multiply, not additively, but combinatorially. A serialization bug in Kafka doesn't just break Kafka; it breaks the Spark job downstream, which stops feeding ClickHouse, which makes the API return stale data, which triggers an alert that says "ClickHouse is slow" when the actual problem is a schema mismatch three hops upstream.

This lesson is integration hell. That's the point. Students will fight Docker networking, version mismatches, serialization bugs, and mysterious "it works on my machine" failures. The pedagogical value isn't in the final running pipeline, it's in the debugging that gets them there.

## Hour 1, Theory: what breaks at the boundaries

### Module A, Exactly-once across system boundaries

In Lesson 8, students achieved exactly-once within a single stream processor. That was the easy version. The hard version is exactly-once across system boundaries, from Postgres to Kafka to Spark to ClickHouse. Each system has its own notion of "committed," and they don't coordinate.

Walk through the end-to-end pipeline and identify every boundary where duplicates or data loss can occur:

1. **Postgres → Debezium (CDC):** Debezium reads the Postgres WAL via a logical replication slot. The slot tracks the LSN (Log Sequence Number) that Debezium has consumed. If Debezium crashes after reading a WAL entry but before committing its offset to Kafka, it will re-read that entry on restart. This produces a duplicate in Kafka. Debezium doesn't provide exactly-once to Kafka, it provides at-least-once.

2. **Debezium → Kafka:** Debezium uses the Kafka producer API. With `enable.idempotence=true` and transactional writes, the producer can avoid duplicates within a single session. But if Debezium restarts and re-reads from the WAL (see above), idempotent production won't help, it's a *new* message from the producer's perspective, with a new sequence number.

3. **Kafka → Spark Structured Streaming:** Spark reads from Kafka using offsets. Spark's checkpointing mechanism records which Kafka offsets have been processed. If Spark crashes after writing to ClickHouse but before checkpointing the offset, it will re-process those events on restart. Duplicate writes to ClickHouse.

4. **Spark → ClickHouse:** ClickHouse doesn't participate in Spark's transaction. There's no two-phase commit between them. Spark writes a batch to ClickHouse, then checkpoints. If the checkpoint fails after the ClickHouse write succeeds, the next micro-batch will re-write the same data.

**The honest answer:** true exactly-once across this entire pipeline is effectively impossible without either (a) making every sink idempotent, or (b) using a transactional protocol that spans all systems (which doesn't exist for this stack). In practice, you design for **at-least-once delivery with idempotent sinks**. This means:

- Every event needs a deterministic, globally unique key (e.g., the Postgres primary key + the operation type + the LSN).
- ClickHouse must be configured to deduplicate on that key (using `ReplacingMergeTree` with a version column, or deduplication at the application layer).
- The API layer must tolerate slightly stale or temporarily duplicated data during recovery windows.

This is the most important conceptual takeaway of the lesson: **exactly-once is a system-level property you construct from at-least-once components plus idempotent sinks.** Anyone who tells you their pipeline is "exactly-once" is either using idempotent sinks (and calling it exactly-once for marketing), or they're wrong.

### Module B, Schema registries and contract evolution

When Debezium captures a change from Postgres, it must serialize it. When Spark reads it from Kafka, it must deserialize it. If the serialization format changes between those two moments, because someone added a column to the Postgres table, the pipeline breaks. Unless you have a schema registry.

**Confluent Schema Registry** stores Avro (or Protobuf, or JSON Schema) schemas and enforces compatibility rules between schema versions. Each Kafka topic is associated with a schema, and each message carries a schema ID in its header. The consumer uses that ID to fetch the correct schema from the registry and deserialize the message.

The compatibility modes matter:

| Mode | What it allows | What it forbids |
|---|---|---|
| **BACKWARD** (default) | New schema can read old data. Adding optional fields, removing fields with defaults. | Adding required fields without defaults. |
| **FORWARD** | Old schema can read new data. Removing fields, adding fields with defaults. | Removing required fields without defaults. |
| **FULL** | Both directions. Most restrictive. | Anything that breaks either direction. |
| **NONE** | Anything goes. | Nothing, but your pipeline will explode. |

For a CDC pipeline, **BACKWARD** compatibility is the right default. Here's why: when you add a nullable column to Postgres, Debezium starts producing messages with the new field. Downstream consumers using the old schema (Spark jobs that haven't been redeployed) need to read these new messages. Under BACKWARD compatibility, the new schema can read old data (old messages lack the new field, consumer uses the default), and the registry ensures the schema change is safe before allowing the producer to register it.

The Avro serialization flow:

```
Producer (Debezium):
  1. Serialize the change event using the current Avro schema
  2. Register the schema with the Schema Registry (if not already registered)
  3. Prepend the schema ID (4 bytes) to the serialized payload
  4. Write to Kafka: [magic byte][schema ID][Avro payload]

Consumer (Spark):
  1. Read message from Kafka
  2. Extract the schema ID from the first 5 bytes
  3. Fetch the corresponding schema from the Registry (cached locally)
  4. Deserialize the Avro payload using that schema
  5. If the consumer expects a different (newer) schema, Avro applies schema resolution rules
     (fill defaults for missing fields, ignore unknown fields)
```

**Why Avro and not Protobuf or JSON?** For CDC, Avro is the pragmatic choice because Debezium has first-class Avro support, the Confluent ecosystem is built around it, and Avro's schema resolution rules handle the kind of evolution CDC produces (added nullable columns, renamed fields via aliases). Protobuf is a fine alternative but the tooling integration with Debezium and Spark is less mature. JSON Schema offers no binary encoding and no meaningful schema resolution, it's a trap for production pipelines.

### Module C, Backpressure propagation

When ClickHouse ingestion slows down (disk I/O spike, merge storm, whatever), what happens upstream? In a well-designed pipeline, the pressure propagates backward:

1. Spark's ClickHouse writer blocks or slows → micro-batch takes longer to complete
2. Spark's Kafka consumer pauses (it won't commit offsets for the slow batch) → consumer lag increases
3. Kafka buffers the data (it's a log, this is what it's for) → lag is visible in metrics
4. Debezium keeps writing to Kafka (Postgres WAL doesn't wait) → Kafka absorbs the pressure
5. Postgres is completely unaffected, it doesn't know or care about anything downstream

This is the beauty of Kafka as a buffer: it **decouples producers and consumers in time**. The failure mode to fear isn't backpressure itself, it's **unbounded lag growth**. If ClickHouse is slow for 5 minutes, you get 5 minutes of lag. If it's slow for 5 hours, you might exhaust Kafka's retention and lose data.

The metrics that make backpressure visible:

- **Consumer lag** (records): `kafka_consumer_group_lag`, the difference between the latest offset in the partition and the consumer's committed offset. This is the single most important metric in a streaming pipeline.
- **Consumer lag** (time): how old is the oldest unconsumed message? Records lag alone is misleading if production rate varies.
- **Processing latency**: how long does each Spark micro-batch take? If it's growing, you're falling behind.
- **Checkpoint duration**: how long does Spark take to write its checkpoint? If this spikes, recovery after failure will be slow.
- **ClickHouse merge activity**: `system.merges`, if background merges are falling behind, insert performance degrades.

### Module D, Observability as a first-class concern

Observability is not "add logging." It's instrumenting the pipeline so that when something breaks at 3am, you can trace the problem from symptom to root cause without SSH-ing into containers and reading logs.

Three pillars, applied to this pipeline:

**Metrics** (Prometheus + Grafana):
- Kafka: consumer lag per partition, producer throughput, request latency
- Spark: micro-batch duration, input rows/sec, state store size
- ClickHouse: insert rows/sec, merge lag, query latency
- Custom: end-to-end latency (timestamp in Postgres row vs. timestamp when queryable in ClickHouse)

**Logs** (structured, always):
- Every component logs in JSON with a correlation ID that traces an event from Postgres through the entire pipeline
- Debezium: connector status, snapshot progress, WAL position
- Spark: batch metadata, offset ranges per batch
- ClickHouse: insert exceptions, slow queries

**Traces** (optional for this lesson, but mention it):
- Distributed tracing (OpenTelemetry) can follow an individual event from OLTP insert to API response. For a 3-hour lesson, this is aspirational, but students should know it exists.

The Grafana dashboard students build is the deliverable, and it must answer these questions at a glance:

1. Is data flowing? (throughput > 0 at every stage)
2. How far behind is each stage? (lag metrics)
3. Where is the bottleneck? (whichever stage has growing lag)
4. How long from INSERT to queryable? (end-to-end latency)

---

## Hour 2, Practical: wire it all together

### The architecture

```
┌──────────┐    ┌──────────┐    ┌─────────┐    ┌───────────┐    ┌────────────┐    ┌─────────┐
│ Postgres │───▶│ Debezium │───▶│  Kafka  │───▶│   Spark   │───▶│ ClickHouse │───▶│ FastAPI │
│  (OLTP)  │ CDC│(Kafka    │    │  + Schema│    │Structured │    │  (OLAP)    │    │  (API)  │
│          │    │ Connect) │    │ Registry │    │ Streaming │    │            │    │         │
└──────────┘    └──────────┘    └─────────┘    └───────────┘    └────────────┘    └─────────┘
                                                                                       │
                                                                               ┌───────┴───────┐
                                                                               │    Grafana     │
                                                                               │  (dashboard)   │
                                                                               └───────────────┘
```

All of this runs in a single Docker Compose file provided by the instructor. Students do not write the Compose file, that's ops work and not the learning objective. The Compose file contains:

- **Postgres 16** with `wal_level=logical` pre-configured
- **Kafka (KRaft mode)**, single broker, no ZooKeeper
- **Confluent Schema Registry**, connected to the Kafka broker
- **Kafka Connect** with Debezium Postgres connector plugin pre-installed
- **Spark** (master + 1 worker) with PySpark and the Kafka + Avro packages
- **ClickHouse** single node
- **Grafana** with Prometheus as a datasource, pre-provisioned but dashboards empty
- **Prometheus** scraping Kafka (JMX exporter), Spark (metrics endpoint), and ClickHouse (built-in Prometheus endpoint)

That's 9 containers. Docker on an 8GB RAM laptop will struggle. Warn students: 16GB minimum, close Slack and Chrome.

### Setup (15 min)

Students clone the repo, run `docker compose up -d`, and wait for all services to be healthy. This will not go smoothly. Common issues:

- Kafka Connect failing to start because Kafka isn't ready yet (fix: healthcheck with dependency in Compose, but some students will have older Docker versions)
- Schema Registry can't connect to Kafka (fix: wait for Kafka to be fully in KRaft mode)
- Port conflicts (fix: students check for local Postgres/Kafka instances)

Budget these 15 minutes for fighting Docker. If a student is stuck on Docker after 10 minutes, have them pair with someone whose environment works. The goal is not Docker mastery, it's pipeline mastery.

Once services are up, students verify each one:

```bash
# Postgres
docker exec -it postgres psql -U postgres -c "SELECT 1"

# Kafka
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Schema Registry
curl http://localhost:8081/subjects

# ClickHouse
docker exec -it clickhouse clickhouse-client --query "SELECT 1"

# Grafana
# Open http://localhost:3000 (admin/admin)
```

### Phase 1, Set up the source schema and CDC (20 min)

Create the source tables in Postgres. Use the `orders` table from Lesson 1, extended with a few more columns to make the schema change exercise interesting later:

```sql
CREATE TABLE orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Register the Debezium connector via the Kafka Connect REST API:

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "topic.prefix": "cdc",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_orders",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "string"
  }
}'
```

Key configuration decisions to explain:

- **AvroConverter** for both key and value, this registers schemas automatically in the Schema Registry. Students can verify by calling `curl http://localhost:8081/subjects` and seeing `cdc.public.orders-key` and `cdc.public.orders-value` appear.
- **`decimal.handling.mode: string`**, Avro's decimal logical type is finicky across the ecosystem. Encoding `NUMERIC` as a string is ugly but interoperable. In production you'd use `decimal` with proper precision, but today we pick the battle that matters (schema evolution) and skip the one that doesn't (Avro decimal encoding).
- **`tombstones.on.delete: true`**, deletes produce a tombstone message (null value) in Kafka. This matters for compacted topics and for downstream consumers that need to handle deletes.

Insert a few test rows and verify they appear in Kafka:

```bash
docker exec -it postgres psql -U postgres -c \
  "INSERT INTO orders (customer_id, product_id, amount) VALUES (1, 100, 29.99), (2, 101, 49.99)"

# Consume from Kafka (using the Avro console consumer)
docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic cdc.public.orders \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081
```

Students should see the Debezium change event envelope with `before`, `after`, `source`, `op`, and `ts_ms` fields. Take a moment to examine this structure, it's not just the row data. The `source` field contains the LSN, transaction ID, and connector metadata. The `op` field is `c` (create), `u` (update), `d` (delete), or `r` (snapshot read). This metadata is what makes CDC different from a simple data dump.

### Phase 2, Spark Structured Streaming from Kafka to ClickHouse (25 min)

This is where students write code. The Spark job reads from the Kafka topic, deserializes the Avro envelope, extracts the `after` struct (the new row state), and writes to ClickHouse.

First, create the ClickHouse target table:

```sql
CREATE TABLE orders (
    id Int64,
    customer_id Int32,
    product_id Int32,
    amount String,
    status String,
    created_at DateTime64(3, 'UTC'),
    _cdc_op String,
    _cdc_ts DateTime64(3, 'UTC'),
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;
```

Why `ReplacingMergeTree`? Because the pipeline is at-least-once. If Spark re-processes a micro-batch after a failure, it will re-insert rows into ClickHouse. `ReplacingMergeTree` deduplicates rows with the same `ORDER BY` key, keeping the one with the highest `_version` (which we'll set to the Kafka offset or the Debezium LSN). Deduplication happens during background merges, so queries may temporarily see duplicates, but `FINAL` queries or `argMax` patterns resolve them.

The PySpark job:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.avro.functions import from_avro
import requests

spark = SparkSession.builder \
    .appName("orders-pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()

# Fetch the latest value schema from the Schema Registry
schema_registry_url = "http://schema-registry:8081"
subject = "cdc.public.orders-value"
response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
avro_schema = response.json()["schema"]

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cdc.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# The Kafka message value is: [magic byte (1)][schema ID (4)][Avro payload]
# Strip the 5-byte header before deserializing
parsed = df.select(
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    from_avro(expr("substring(value, 6)"), avro_schema).alias("envelope")
)

# Extract the 'after' struct (the new row state)
# For deletes, 'after' is null, handle that downstream
orders = parsed.select(
    col("kafka_offset"),
    col("envelope.after.id").alias("id"),
    col("envelope.after.customer_id").alias("customer_id"),
    col("envelope.after.product_id").alias("product_id"),
    col("envelope.after.amount").alias("amount"),
    col("envelope.after.status").alias("status"),
    col("envelope.after.created_at").alias("created_at"),
    col("envelope.op").alias("_cdc_op"),
    col("envelope.ts_ms").alias("_cdc_ts"),
    col("kafka_offset").alias("_version")
).filter(col("envelope.op") != "d")  # Skip deletes for now; handle properly in production

# Write to ClickHouse using JDBC
def write_to_clickhouse(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "orders") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

orders.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/orders") \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()
```

**Expect this to not work on the first try.** Common issues students will hit:

- **Missing JDBC driver**: the ClickHouse JDBC jar isn't in the Spark classpath. Students need to add it via `spark.jars` config or download it into the Spark container. This is a real production problem, not a contrived one.
- **Avro deserialization errors**: the 5-byte header stripping (`substring(value, 6)`) is the Confluent wire format. If students use a different Avro deserializer that expects a raw Avro payload (not Confluent-framed), they'll get garbage. This is the #1 Avro-with-Kafka debugging issue in the real world.
- **Schema mismatch**: the Avro schema from the registry describes the Debezium envelope, which wraps the row data in `before`/`after` structs. Students who try to deserialize the payload as a flat row will get confusing errors.
- **ClickHouse type mismatches**: Postgres `NUMERIC(10,2)` serialized as Avro string → ClickHouse `String` works. But if someone tries to map it to `Decimal`, the string representation might not parse correctly.

Let students fight these issues. Circulate and give hints, not answers. Each of these bugs teaches a boundary lesson that no amount of theory can replace.

### Phase 3, The API layer (10 min)

A minimal FastAPI application that queries ClickHouse and exposes the data:

```python
from fastapi import FastAPI
from clickhouse_driver import Client
from datetime import datetime, timedelta

app = FastAPI()
ch = Client(host="clickhouse", port=9000)

@app.get("/orders/summary")
def order_summary(minutes: int = 5):
    """Revenue and order count for the last N minutes."""
    query = """
        SELECT
            toStartOfMinute(created_at) AS minute,
            count() AS order_count,
            sum(toDecimal64(amount, 2)) AS total_revenue
        FROM orders FINAL
        WHERE created_at >= now() - INTERVAL {minutes:UInt32} MINUTE
        GROUP BY minute
        ORDER BY minute
    """
    rows = ch.execute(query, {"minutes": minutes})
    return [
        {"minute": str(r[0]), "order_count": r[1], "total_revenue": float(r[2])}
        for r in rows
    ]

@app.get("/orders/latest")
def latest_orders(limit: int = 10):
    """Most recent orders."""
    query = """
        SELECT id, customer_id, product_id, amount, status, created_at
        FROM orders FINAL
        ORDER BY created_at DESC
        LIMIT {limit:UInt32}
    """
    rows = ch.execute(query, {"limit": limit})
    return [
        {
            "id": r[0], "customer_id": r[1], "product_id": r[2],
            "amount": r[3], "status": r[4], "created_at": str(r[5])
        }
        for r in rows
    ]

@app.get("/health")
def health():
    """Pipeline health: is data flowing?"""
    query = """
        SELECT
            max(created_at) AS latest_event,
            dateDiff('second', max(created_at), now()) AS lag_seconds
        FROM orders FINAL
    """
    rows = ch.execute(query)
    latest, lag = rows[0]
    return {
        "latest_event": str(latest),
        "lag_seconds": lag,
        "status": "healthy" if lag < 60 else "degraded"
    }
```

Note the `FINAL` keyword in every ClickHouse query. This forces deduplication at query time (merging rows with the same `ORDER BY` key from `ReplacingMergeTree`). It's slower than reading without `FINAL`, but it guarantees correct results. In production, you'd use `FINAL` selectively or use `argMax` aggregation patterns instead. For this lesson, correctness beats performance.

### Phase 4, Load generation and verification (10 min)

Students run a simple load generator against Postgres and verify data flows end-to-end:

```python
import psycopg
import random
import time

conn = psycopg.connect("postgresql://postgres:postgres@localhost:5432/postgres")
conn.autocommit = True

while True:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, product_id, amount, status) "
            "VALUES (%s, %s, %s, %s)",
            (random.randint(1, 1000), random.randint(1, 50),
             round(random.uniform(5.0, 500.0), 2),
             random.choice(["pending", "confirmed", "shipped"]))
        )
    time.sleep(0.1)  # ~10 rows/sec, enough to see flow, not enough to stress anything
```

Verification checklist:

1. Insert rows in Postgres → see them in the Kafka topic (console consumer)
2. Kafka topic → see them consumed by Spark (Spark UI at port 4040, check streaming tab)
3. Spark → see them in ClickHouse (`SELECT count() FROM orders`)
4. ClickHouse → see them in the API (`curl http://localhost:8000/orders/latest`)

If all four checks pass, the pipeline is running. If any check fails, that's where the debugging starts. This is the lesson.

---

## Hour 3, The schema change exercise and observability

### The schema change: adding a `discount_code` column

This is the centerpiece of the lesson. The change is deliberately simple in isolation, add a nullable `VARCHAR` column, but propagating it through six systems without downtime is where the complexity lives.

**The scenario:** product requirements say we need to track discount codes on orders. The column is nullable (not all orders have discounts) and has no default value. This is the most common schema change in practice, which is exactly why it's the right one to teach.

**Step 1, Change the source (Postgres):**

```sql
ALTER TABLE orders ADD COLUMN discount_code VARCHAR(50);
```

This is instant in Postgres (nullable columns with no default are metadata-only changes, no table rewrite). Production traffic continues.

Now start inserting orders with discount codes:

```sql
INSERT INTO orders (customer_id, product_id, amount, status, discount_code)
VALUES (42, 100, 29.99, 'confirmed', 'SUMMER20');
```

**What happens at each downstream stage, walk through this with the class, one layer at a time:**

### Step 2, Debezium detects the schema change

Debezium reads the next WAL entry and notices the new column. It generates a new Avro schema for the `cdc.public.orders-value` subject that includes `discount_code` as a nullable union type (`["null", "string"]`). It attempts to register this schema with the Schema Registry.

**Will the Schema Registry accept it?** Under BACKWARD compatibility (the default): yes. The new schema adds an optional field. A consumer using the old schema can still read messages produced with the new schema, it simply ignores the unknown field. And a consumer using the new schema can read old messages, the missing field defaults to `null`.

Students should verify the new schema version:

```bash
# Check schema versions
curl http://localhost:8081/subjects/cdc.public.orders-value/versions

# Fetch the new version
curl http://localhost:8081/subjects/cdc.public.orders-value/versions/latest | jq '.schema | fromjson'
```

They should see that `discount_code` appears as a union type: `["null", "string"]` with a default of `null`. This is Avro's way of representing nullable fields, and it's the mechanism that makes backward-compatible evolution work.

**What if we had set compatibility to FULL and the field wasn't nullable?** The registration would be rejected. The Schema Registry acts as a gate, bad schema changes are caught here, not at 3am when a consumer crashes. This is the value of the registry.

### Step 3, Kafka: messages with two schema versions coexist

At this point, the Kafka topic contains a mix of messages: some serialized with schema v1 (no `discount_code`), some with schema v2 (with `discount_code`). Each message carries its schema ID, so any consumer can deserialize any message, it just needs to fetch the right schema from the registry.

Students should consume a few messages and observe the schema ID changing:

```bash
# Raw consumer showing headers/metadata
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.orders \
  --from-beginning \
  --property print.headers=true \
  --max-messages 5
```

The first 5 bytes of each value payload contain the schema ID. Old messages will have ID N, new messages will have ID N+1.

### Step 4, Spark: the consumer must handle both schemas

Here's where it gets interesting. The Spark job was started with the old Avro schema (fetched from the registry at startup). It's now receiving messages with a new schema.

**What happens?** Two scenarios:

**Scenario A, Spark fetched the schema once at startup and doesn't refresh.** Old messages deserialize fine. New messages with `discount_code` also deserialize fine under Avro schema resolution, the extra field is ignored because it's not in the reader schema. Data flows, but `discount_code` is silently dropped. No error, no crash, just silent data loss of the new field.

**Scenario B, Spark is configured to fetch the latest schema from the registry periodically or per batch.** Now it deserializes `discount_code` and attempts to write it to ClickHouse. But the ClickHouse table doesn't have that column yet. The JDBC write fails with a column mismatch error. The micro-batch retries. Consumer lag grows. The pipeline is stuck.

Both scenarios are instructive. Scenario A is the insidious failure, everything looks green, but you're losing data. Scenario B is the loud failure, it's obviously broken, which is actually better because you'll fix it.

**The fix, ordered migration:**

1. Add the column to ClickHouse first (before Spark picks up the new schema):

```sql
ALTER TABLE orders ADD COLUMN discount_code Nullable(String) AFTER status;
```

2. Update the Spark job to include `discount_code` in its projection:

```python
# Add to the select statement
col("envelope.after.discount_code").alias("discount_code"),
```

3. Restart the Spark job (or if using schema-on-read, it picks up the new column automatically on the next batch).

4. Update the FastAPI endpoints to include `discount_code` in the response.

**The general rule for forward-compatible schema changes:** migrate downstream before upstream. Add the column to the sink (ClickHouse) before the source (Postgres) starts producing it. This way, when the new-schema messages arrive, the sink is already ready. For backward-incompatible changes (removing a column, changing a type), migrate upstream first and let the old data drain through.

Students should perform this migration live, with the load generator running. At no point should the pipeline stop. Lag may spike briefly during the Spark restart, but it should recover.

### Step 5, Verify the complete propagation

```bash
# Insert an order with a discount code in Postgres
docker exec -it postgres psql -U postgres -c \
  "INSERT INTO orders (customer_id, product_id, amount, status, discount_code) \
   VALUES (99, 200, 149.99, 'confirmed', 'WELCOME10')"

# Verify it reaches ClickHouse
docker exec -it clickhouse clickhouse-client --query \
  "SELECT id, amount, discount_code FROM orders FINAL WHERE discount_code IS NOT NULL"

# Verify it's in the API
curl http://localhost:8000/orders/latest
```

If `WELCOME10` appears in the API response with the correct order data, the schema change has propagated through all six systems without downtime. That's the exercise.

### Building the Grafana dashboard (20 min)

Students build a Grafana dashboard with the following panels. The data sources (Prometheus, ClickHouse) are pre-configured in Grafana; students create the panels.

**Panel 1, Kafka consumer lag (Prometheus):**

```promql
# Consumer lag for the Spark consumer group
kafka_consumergroup_lag{group="spark-orders-pipeline", topic="cdc.public.orders"}
```

This is the single most important metric. If lag is zero and stable, data is flowing at the rate it's produced. If lag is growing, the consumer can't keep up.

**Panel 2, Throughput at each stage (Prometheus + ClickHouse):**

```promql
# Kafka: messages produced per second
rate(kafka_topic_partition_current_offset{topic="cdc.public.orders"}[1m])
```

```sql
-- ClickHouse: rows inserted per second (query as a Grafana ClickHouse datasource)
SELECT
    toStartOfMinute(now()) AS time,
    count() / 60 AS rows_per_sec
FROM orders
WHERE _cdc_ts >= now() - INTERVAL 5 MINUTE
GROUP BY time
ORDER BY time
```

**Panel 3, End-to-end latency (ClickHouse):**

```sql
-- Time between Postgres insert (created_at) and ClickHouse insert (estimated from _cdc_ts)
SELECT
    toStartOfMinute(_cdc_ts) AS minute,
    avg(toUnixTimestamp64Milli(_cdc_ts) - toUnixTimestamp64Milli(created_at)) AS avg_latency_ms,
    quantile(0.99)(toUnixTimestamp64Milli(_cdc_ts) - toUnixTimestamp64Milli(created_at)) AS p99_latency_ms
FROM orders FINAL
WHERE _cdc_ts >= now() - INTERVAL 10 MINUTE
GROUP BY minute
ORDER BY minute
```

This metric is approximate (it uses the Debezium event timestamp as a proxy for "when it reached ClickHouse"), but it's good enough to see latency trends and spikes.

**Panel 4, Spark micro-batch duration (Prometheus):**

```promql
spark_streaming_lastProgress_batchDuration{application="orders-pipeline"}
```

If this number is consistently greater than the trigger interval (10 seconds), the pipeline is falling behind. Each batch takes longer than the interval between batches, so lag will grow unboundedly.

**Panel 5, Pipeline health (Stat panel):**

A single stat panel showing the API health endpoint result, `healthy` in green if lag is under 60 seconds, `degraded` in yellow/red otherwise.

### Final 20 minutes, Controlled chaos and discussion

With the dashboard visible to everyone, run a few experiments:

**Experiment 1, Spike the load:**

Change the load generator sleep from 0.1s to 0 (no delay). Watch consumer lag spike on the dashboard. Watch Spark batch duration increase. Watch ClickHouse insert rate climb. Then restore the sleep and watch lag drain. This is backpressure in action.

**Experiment 2, Stop Spark:**

`docker stop spark-worker`. Watch lag grow linearly while Debezium and Kafka continue unaffected. The API starts returning stale data (the `/health` endpoint shows increasing lag). Restart Spark and watch it catch up, processing the backlog faster than real-time because the accumulated messages are processed in rapid micro-batches.

**Experiment 3, Break the schema:**

Try registering an incompatible schema change (e.g., removing a required field). The Schema Registry should reject it. Show the error. This is the safety net working as designed.

**Class discussion:**

- What is the weakest link in this pipeline? (Usually the stream processor, it's the most complex component and the one with the most configuration surface area.)
- What would you monitor if you could only have three metrics? (Consumer lag, end-to-end latency, error rate. Everything else is a refinement of these three.)
- What's missing from this pipeline for production? (Authentication, encryption, retry policies with DLQs, schema change automation, capacity planning, multi-datacenter replication, proper CI/CD for the pipeline code.)

---

## Take-home deliverable

A Git repository submitted as a pull request, containing:

- All pipeline code: Debezium connector config, PySpark job, ClickHouse schema, FastAPI application, load generator
- A `README.md` documenting: how to start the pipeline (`docker compose up`), how to verify data flow, the schema change procedure step by step, and a screenshot of the Grafana dashboard under load
- An `AGENTS.md` or `CLAUDE.md` file: students are encouraged to use AI assistants for debugging and development. This file documents which parts were AI-assisted, what prompts were effective, and what the AI got wrong (there will be things, especially around Avro serialization and ClickHouse types)
- The Grafana dashboard exported as JSON (Grafana → Share → Export)

AI assistance is explicitly encouraged for this lesson. The integration surface area is too large for anyone to hold entirely in their head. The skill being tested is not "can you memorize the ClickHouse JDBC driver class name", it's "can you design, debug, and operate a multi-system pipeline." Using an AI assistant to handle boilerplate and debug serialization errors is a legitimate engineering practice, and students should learn to do it well rather than pretend they don't.

**What gets graded:**

1. Does the pipeline work end-to-end? (INSERT in Postgres → visible in API)
2. Does the schema change propagate without downtime?
3. Does the Grafana dashboard show lag, throughput, and latency at every stage?
4. Is the `README.md` clear enough that someone else could reproduce the setup?
5. Does the `AGENTS.md` honestly document AI usage?

What doesn't get graded: code elegance, performance optimization, or handling edge cases like deletes and schema rollbacks. There's a lesson for that (Lesson 12). This lesson's bar is: **it works, it's observable, and you can evolve it without breaking it.**
