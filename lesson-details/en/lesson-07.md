# Lesson 7, Stream processing I: stateless transformations and windowing

Lessons 5 and 6 built the plumbing: CDC captures changes from the OLTP source, Kafka durably transports them. But Kafka is just a pipe. The events sit in topics, waiting for something to actually compute over them. That "something" is a stream processor. This lesson is where students stop moving data and start transforming it in flight.

The tech choice is PySpark Structured Streaming. Not Flink, not Faust, not Kafka Streams. Spark, because: (a) students will encounter it in nearly every data engineering job; (b) the DataFrame API is familiar from batch work, so the conceptual leap is "batch processing, but the input never ends"; (c) the micro-batch execution model makes windowing and watermarks *visible* in a way continuous engines obscure. The micro-batch nature is not a limitation here -- it's a pedagogical advantage. In Lesson 9, students will compare Spark against Flink and discover where micro-batch hurts. For now, micro-batch is the teaching tool.

## Hour 1 -- Theory: how stream processing actually works

### Module A -- The topology: sources, operators, sinks

Every stream processing job is a directed acyclic graph (DAG). Draw it on the board:

```
Source (Kafka topic) --> map --> filter --> window aggregate --> Sink (console / Kafka / file)
```

**Source:** where events enter the pipeline. In our case, a Kafka topic containing CDC events from Lesson 5. The source tracks *where* it is in the stream (Kafka offsets), so it can resume after failure.

**Operators:** transformations applied to each event (or batch of events, or window of events). They come in two flavors that students must distinguish clearly:

- **Stateless** -- output depends only on the current event. `map`, `filter`, `flatMap`. No memory between events. Trivially parallelizable, trivially restartable.
- **Stateful** -- output depends on accumulated information across events. Aggregations, joins, sessionization. Requires state management, checkpointing, exactly-once semantics. That's Lesson 8 -- don't go deep here, just establish the distinction.

**Sink:** where results go. Console for debugging, Kafka for downstream consumers, files for batch integration, a database for serving.

This topology model applies to every stream processor: Spark, Flink, Kafka Streams, Faust. The APIs differ; the mental model is identical.

### Module B -- Spark's programming model: DataFrames that never end

This is students' first contact with Spark. Don't assume any Spark background. Spend real time here -- if they don't internalize the programming model, the rest of the lesson is rote typing.

**DataFrames.** A Spark DataFrame is a distributed table -- rows and typed columns. Students already know DataFrames from pandas or DuckDB. Spark DataFrames look similar but are *lazy*: calling `.filter()` or `.select()` doesn't execute anything. It builds a logical plan. Execution only happens when you trigger it -- in batch Spark, that's an action like `.collect()` or `.write`. In streaming, the trigger is the micro-batch loop.

**readStream vs. read.** In batch Spark, you do `spark.read.format("kafka").load()` -- this reads a fixed snapshot of the topic. In streaming, you do `spark.readStream.format("kafka").load()` -- this creates an *unbounded* DataFrame that continuously ingests new data. The API is nearly identical. The difference is that `readStream` produces a streaming DataFrame, and most DataFrame operations work on it unchanged.

Show the minimal example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lesson-07") \
    .master("local[*]") \
    .getOrCreate()

# Batch read -- finite
df_batch = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders-cdc") \
    .load()

# Streaming read -- infinite
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders-cdc") \
    .load()
```

Both return DataFrames with the same schema: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`. The `value` column is binary -- that's the raw Kafka message. Students must parse it.

**writeStream and triggers.** A streaming query starts when you call `writeStream.start()`. The trigger controls how often Spark pulls new data and processes it:

```python
query = df_stream \
    .writeStream \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()
```

With `processingTime="5 seconds"`, Spark wakes up every 5 seconds, reads all new events since the last batch, processes them as a batch, writes results, and goes back to sleep. This is the micro-batch loop. Each iteration is a complete Spark job -- plan, optimize, execute. The 5-second interval is a tuning knob: shorter means lower latency, longer means higher throughput (bigger batches amortize overhead).

**Key insight to drive home:** a Structured Streaming job is conceptually an infinite loop of batch queries. Each iteration processes only the new data. Spark tracks progress (offsets) in a checkpoint directory so it can resume exactly where it left off after failure. Students who understand this can reason about everything else -- windowing, watermarks, state -- as additions on top of this loop.

### Module C -- Stateless transformations: map, filter, flatMap

Stateless transforms operate event-by-event. No accumulation, no memory, no state. In Spark, they look identical to batch DataFrame operations:

```python
from pyspark.sql.functions import col, from_json, schema_of_json

# Parse the Kafka value from JSON
orders = df_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

# filter: keep only orders above $50
big_orders = orders.filter(col("amount") > 50)

# map (select/withColumn): extract and transform fields
enriched = orders.select(
    col("order_id"),
    col("amount"),
    col("created_at"),
    (col("amount") * 0.08).alias("tax")
)
```

There's no explicit `map()` function in the DataFrame API -- `select()` and `withColumn()` are the map. `filter()` is filter. `flatMap` doesn't exist directly on DataFrames -- you use `explode()` to turn one row into many (e.g., an order with an array of line items becomes one row per line item).

These are boring on purpose. Stateless transforms are the easy part. Students should be comfortable that streaming DataFrames work just like batch DataFrames for these operations. The hard part starts now.

### Module D -- Event time vs. processing time

This is the single most important concept in stream processing. Get it wrong and every aggregation is wrong.

**Processing time** is when the stream processor sees the event. It's the wall clock on the machine running Spark.

**Event time** is when the event actually happened -- the `created_at` timestamp in the order record, set by the source system.

Why are they different? Because events can be delayed. Network latency, Kafka consumer lag, CDC replication lag, the source system batching writes. An order placed at 14:00:00 might arrive at the stream processor at 14:00:03, or 14:02:00 if there's a backlog.

If you window by processing time, an order placed at 13:59:59 that arrives at 14:00:03 lands in the 14:00-14:05 window instead of the 13:55-14:00 window. Your "revenue per 5-minute window" numbers are wrong. Not slightly wrong -- fundamentally wrong, because they reflect when your pipeline happened to process events, not when business activity occurred.

**This is why stream processors window by event time.** Spark Structured Streaming uses event time by default when you specify a timestamp column in your windowing expression. Processing-time windowing exists (`current_timestamp()`), but it's the wrong default for most business logic.

Pose the concrete scenario: your e-commerce site has a flash sale from 12:00 to 12:05. You want total revenue during the sale. If your pipeline has 30 seconds of lag, processing-time windows would attribute the last 30 seconds of sale revenue to the 12:05-12:10 window -- the sale looks less successful than it was, and the post-sale period looks inflated. Event-time windows get this right.

### Module E -- Windowing: tumbling, sliding, session

Once you've chosen event time, you need to define how to group events into finite chunks. That's windowing.

**Tumbling windows** are fixed-size, non-overlapping. A 5-minute tumbling window starting at the hour gives you [12:00, 12:05), [12:05, 12:10), [12:10, 12:15), etc. Every event belongs to exactly one window. Use case: "revenue per 5-minute interval."

```python
from pyspark.sql.functions import window, sum as _sum

revenue = orders \
    .groupBy(window(col("created_at"), "5 minutes")) \
    .agg(_sum("amount").alias("total_revenue"))
```

Spark's `window()` function takes the event-time column and the window duration. It creates a struct column with `start` and `end` fields. That's the window.

**Sliding windows** are fixed-size but overlapping. A 10-minute window that slides every 2 minutes gives you [12:00, 12:10), [12:02, 12:12), [12:04, 12:14), etc. Every event belongs to multiple windows (in this case, 5 windows). Use case: "rolling 10-minute average, updated every 2 minutes."

```python
# 10-minute window, sliding every 2 minutes
rolling = orders \
    .groupBy(window(col("created_at"), "10 minutes", "2 minutes")) \
    .agg(_sum("amount").alias("rolling_revenue"))
```

The second argument is the window size, the third is the slide interval. If they're equal, it's a tumbling window. Students should understand that sliding windows duplicate work -- each event is processed once per window it belongs to.

**Session windows** are defined by a gap of inactivity. Events within a gap duration of each other belong to the same session; a gap longer than the threshold starts a new session. Use case: "user browsing sessions, where a session ends after 30 minutes of inactivity."

```python
# Session window with 30-minute gap
sessions = events \
    .groupBy(
        col("user_id"),
        session_window(col("event_time"), "30 minutes")
    ) \
    .agg(count("*").alias("events_in_session"))
```

Session windows are harder than tumbling/sliding because their boundaries depend on the data. You can't predetermine when a session ends -- you have to wait for the gap. This is where watermarks (next module) become critical.

### Module F -- Watermarks: the completeness/latency tradeoff

Here's the problem: you're computing revenue for the [12:00, 12:05) window. It's now 12:06. Is the window complete? Maybe -- unless a late event from 12:04 is still in transit. Do you emit the result now (fast but potentially incomplete) or wait (complete but slow)? How long do you wait? Forever?

This is the completeness/latency tradeoff. Watermarks are the mechanism for resolving it.

A **watermark** is a threshold that says: "I assert that no events with event time earlier than T will ever arrive." In Spark, you define it as an allowed lateness relative to the maximum event time seen so far:

```python
orders_with_watermark = orders \
    .withWatermark("created_at", "10 minutes")
```

This means: once Spark has seen an event with `created_at = 12:15:00`, the watermark advances to `12:05:00` (15 minutes minus 10 minutes of allowed lateness). Any window that ends at or before `12:05:00` is now considered complete. Spark emits the result and drops state for that window. Events arriving after this point with `created_at` before `12:05:00` are **dropped** -- they're too late.

Walk through a concrete timeline:

| Micro-batch | Max event time seen | Watermark (10 min lateness) | Windows finalized |
|---|---|---|---|
| Batch 1 | 12:08:00 | 11:58:00 | none |
| Batch 2 | 12:12:00 | 12:02:00 | none |
| Batch 3 | 12:16:00 | 12:06:00 | [12:00, 12:05) emitted |
| Batch 4 | 12:21:00 | 12:11:00 | [12:05, 12:10) emitted |

If an event with `created_at = 12:03:00` arrives in batch 4, it's past the watermark (12:11:00 > 12:03:00) and gets silently dropped. The [12:00, 12:05) window was already emitted and its state is gone.

**The tradeoff is explicit:**

- **Short watermark (e.g., 1 minute):** low latency (windows close quickly), but late events are dropped aggressively. Good when data arrives roughly on time.
- **Long watermark (e.g., 1 hour):** tolerant of late data, but windows stay open for an hour. State memory grows (Spark must keep all open windows in memory), and downstream consumers wait longer for results.
- **No watermark:** windows never close. State grows forever. Eventually you run out of memory and crash. This is a real failure mode students will encounter if they forget the watermark.

**Why watermarks are approximate:** the watermark advances based on the max event time *Spark has seen*. If one Kafka partition is lagging behind (say, it has events from 12:03 while another partition is at 12:16), the watermark should ideally track the slowest partition. Spark's watermark is global across all partitions -- it uses the max event time across all data, which means a stalled partition won't hold back the watermark. This can cause events from the slow partition to be dropped when it catches up. This is a real operational gotcha that students should understand, even if they won't hit it in this lab.

---

## Hour 2 -- Practical: build the windowed aggregation pipeline

### Setup (15 min)

**PySpark installation:** `pip install pyspark`. That's it. No Hadoop, no YARN, no cluster. Spark ships with a standalone mode that runs locally in a single JVM. Students need Java 11+ installed (`brew install openjdk@11` or equivalent). Verify with:

```bash
python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[*]').getOrCreate().version)"
```

If this prints a version number, the setup works. If it fails, it's almost always a missing `JAVA_HOME`. Don't let students debug Java installations for 30 minutes -- have a troubleshooting cheat sheet ready.

**Kafka:** reuse the Kafka setup from Lesson 6 (KRaft mode, single broker). Students should have the `orders-cdc` topic populated with CDC events from Lesson 5. If they don't, provide a script that generates synthetic order events:

```python
import json
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
topic = "orders-cdc"

base_time = datetime(2025, 1, 15, 12, 0, 0)

for i in range(10000):
    # Simulate event-time skew: most events are near-realtime,
    # but ~5% are delayed by 1-10 minutes
    if random.random() < 0.05:
        event_time = base_time + timedelta(seconds=i) - timedelta(minutes=random.randint(1, 10))
    else:
        event_time = base_time + timedelta(seconds=i)

    event = {
        "order_id": i,
        "customer_id": random.randint(1, 1000),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "status": "completed",
        "created_at": event_time.isoformat()
    }
    producer.produce(topic, key=str(i), value=json.dumps(event))

    if i % 100 == 0:
        producer.flush()
        time.sleep(0.1)  # pace the production so streaming has something to chew on

producer.flush()
print(f"Produced {10000} events to {topic}")
```

Note the deliberate skew: 5% of events have event times 1-10 minutes in the past. These are the "late events" students will observe hitting (or missing) the watermark.

**Schema definition for Spark:**

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("created_at", TimestampType())
])
```

### Phase 1 -- Read from Kafka and parse (15 min)

Students write the ingestion code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder \
    .appName("lesson-07-windowed-revenue") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders-cdc") \
    .option("startingOffsets", "earliest") \
    .load()

orders = raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")
```

Two things to call out explicitly:

1. **`spark.jars.packages`** -- this downloads the Kafka connector JAR at runtime. First run will be slow (Maven download). Students can also pre-download the JAR and use `spark.jars` instead.
2. **`startingOffsets = "earliest"`** -- reads from the beginning of the topic. Without this, Spark defaults to `latest` and students see nothing on the first run, which is confusing.

Have students write to the console first to verify the pipeline works:

```python
query = orders.writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```

They should see parsed order rows printing every 5 seconds. If not, debug now -- don't proceed with a broken pipeline.

### Phase 2 -- Tumbling window aggregation (20 min)

Now add the windowing:

```python
from pyspark.sql.functions import window, sum as _sum, count, avg

windowed_revenue = orders \
    .withWatermark("created_at", "10 minutes") \
    .groupBy(window(col("created_at"), "5 minutes")) \
    .agg(
        _sum("amount").alias("total_revenue"),
        count("*").alias("order_count"),
        avg("amount").alias("avg_order_value")
    )

query = windowed_revenue.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Output modes matter.** Explain the three:

- **`append`** -- only emit rows when the window is finalized (watermark has passed). This is the "correct" mode for downstream systems that can't handle updates. But it means no output until the watermark passes, which feels broken during development.
- **`update`** -- emit rows whenever they change (partial results for open windows, final results for closed windows). Good for development and for sinks that can handle upserts.
- **`complete`** -- emit the entire result table every micro-batch. Only works for aggregations. Expensive for large state.

Students should run with `update` mode first so they can see partial window results as they accumulate, then switch to `append` and observe the delay -- results only appear after the watermark passes the window boundary. That delay is the lateness tolerance working as designed.

**Key exercise:** have students modify the trigger interval (1 second, 5 seconds, 30 seconds) and observe the effect. With a 1-second trigger, they see many micro-batches with few events each. With 30 seconds, they see fewer batches with more events. The window results are the same either way -- the trigger controls latency, not correctness. This is a critical insight: **the trigger interval and the window size are independent parameters.** Students who confuse them will produce wrong pipelines.

### Phase 3 -- Observe watermark behavior with late events (25 min)

This is the core exercise. Students must see watermarks in action -- not just as theory, but as visible behavior.

**Step 1:** Run the pipeline with the 10-minute watermark and `append` mode. Observe which windows emit and when.

**Step 2:** While the pipeline is running, inject deliberately late events:

```python
# Separate script: inject events that are 15 minutes late
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

# Current "event time frontier" is around base_time + 10000 seconds
# Inject events way in the past
late_time = datetime(2025, 1, 15, 12, 5, 0)  # 12:05, window [12:05, 12:10)

for i in range(50):
    event = {
        "order_id": 100000 + i,
        "customer_id": 9999,
        "amount": 999.99,
        "status": "completed",
        "created_at": late_time.isoformat()
    }
    producer.produce("orders-cdc", key=str(100000 + i), value=json.dumps(event))

producer.flush()
print("Injected 50 late events at 12:05:00")
```

**What students should observe:** if the watermark has already advanced past 12:10:00 (which it has, given the main producer has been running), these late events are dropped. The [12:05, 12:10) window result, already emitted, does not change. The 50 orders worth $49,999.50 in revenue are simply lost.

This is the teaching moment. Ask: **"Where did those 50 orders go?"** Nowhere. They were silently dropped. No error, no log message (by default), no dead-letter queue. The watermark said "no events before 12:10 will arrive" and these events violated that contract, so Spark discards them.

**Step 3:** Reduce the watermark to 1 minute and re-run. Now even the 5% of events that the producer script delayed by 1-10 minutes will be dropped. Students should see window totals that are lower than expected -- the late events that previously squeaked in under a 10-minute watermark are now too late.

**Step 4:** Increase the watermark to 30 minutes. Now almost everything gets counted, but windows take 30 minutes to close. Students should observe: in `append` mode, no output for 30 minutes. State memory usage grows (visible via `query.lastProgress` which reports state rows).

Students should fill in a table:

| Watermark | Late events captured | Time to first window output | State rows (approx) |
|---|---|---|---|
| 1 minute | | | |
| 10 minutes | | | |
| 30 minutes | | | |

This table is the completeness/latency tradeoff, made concrete with their own data.

### Phase 4 -- Monitor the pipeline (15 min)

Students need to know how to observe a running streaming query. Spark exposes progress metrics:

```python
import json

# After starting the query
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(json.dumps({
            "batch_id": progress["batchId"],
            "num_input_rows": progress["numInputRows"],
            "input_rows_per_second": progress["inputRowsPerSecond"],
            "processed_rows_per_second": progress["processedRowsPerSecond"],
            "state_operators": progress["stateOperators"],
        }, indent=2))
    time.sleep(10)
```

The `stateOperators` field is critical. It reports:

- `numRowsTotal` -- how many state rows the aggregate operator is maintaining (i.e., how many open windows)
- `numRowsUpdated` -- how many state rows changed in the last batch
- `numRowsDroppedByWatermark` -- **how many late events were dropped**

That last field is the smoking gun. Students can now measure exactly how many events their watermark is discarding. If the number is zero, the watermark is generous enough. If it's large, they're losing data and need to decide whether that's acceptable.

---

## Hour 3 -- Push further, then reason about it

### Experiment A -- Sliding windows and the cost of overlap (20 min)

Replace the tumbling window with a sliding window:

```python
# 10-minute window, sliding every 1 minute
sliding_revenue = orders \
    .withWatermark("created_at", "10 minutes") \
    .groupBy(window(col("created_at"), "10 minutes", "1 minute")) \
    .agg(
        _sum("amount").alias("total_revenue"),
        count("*").alias("order_count")
    )
```

Each event now belongs to 10 windows (10-minute window / 1-minute slide). Students should observe:

1. **More output rows per batch** -- 10x more window results than tumbling.
2. **Higher state memory** -- `numRowsTotal` in `stateOperators` is ~10x larger.
3. **Longer processing time per batch** -- visible in `processedRowsPerSecond` dropping.

Ask: **"If you need a 1-hour sliding window that slides every 10 seconds, how many windows does each event belong to?"** Answer: 360. Each event is duplicated into 360 windows. State size and processing cost scale linearly with `window_size / slide_interval`. This is why sliding windows with small slide intervals relative to window size are expensive. In production, this is a capacity planning concern, not a code concern.

### Experiment B -- Multiple concurrent aggregations (15 min)

A real pipeline doesn't compute just one aggregation. Students should fork their stream into two outputs:

```python
# Aggregation 1: revenue per 5-minute tumbling window
revenue_5m = orders \
    .withWatermark("created_at", "10 minutes") \
    .groupBy(window(col("created_at"), "5 minutes")) \
    .agg(_sum("amount").alias("total_revenue"))

# Aggregation 2: order count per customer per 15-minute tumbling window
customer_orders = orders \
    .withWatermark("created_at", "10 minutes") \
    .groupBy(
        col("customer_id"),
        window(col("created_at"), "15 minutes")
    ) \
    .agg(count("*").alias("order_count"))

query1 = revenue_5m.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint-revenue") \
    .trigger(processingTime="10 seconds") \
    .start()

query2 = customer_orders.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint-customers") \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()
```

Two things to emphasize:

1. **Checkpoint locations must be different** per query. Checkpoints store offset and state information. Two queries sharing a checkpoint directory will corrupt each other. This is a common mistake.
2. **Both queries share the same Kafka source.** Spark doesn't read from Kafka twice -- the source data is shared. But each query maintains independent state. Independent watermarks would require separate `withWatermark` calls (which they already have, but with the same threshold here).

### Experiment C -- Writing results to Kafka (15 min)

Console output is for debugging. In a real pipeline, results go to Kafka for downstream consumers or to a database for serving. Students write window results to a new Kafka topic:

```python
output = windowed_revenue.select(
    col("window.start").cast("string").alias("key"),
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_revenue"),
        col("order_count"),
        col("avg_order_value")
    )).alias("value")
)

query = output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "revenue-per-window") \
    .option("checkpointLocation", "/tmp/checkpoint-kafka-sink") \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()
```

The `key` and `value` columns must be strings or binary -- Kafka's format requirement. Students should consume from `revenue-per-window` using their Lesson 6 consumer to verify results are flowing end-to-end.

**Critical detail:** the checkpoint directory is what gives you fault tolerance. Spark writes the offset range and state for each batch to the checkpoint *before* committing the output. On restart, it reads the checkpoint, skips already-processed offsets, and resumes. Students should kill the pipeline (Ctrl-C), restart it, and verify it picks up where it left off without reprocessing or duplication.

### Experiment D -- The "no watermark" failure (10 min)

Remove the watermark and run the pipeline with `append` mode:

```python
# Deliberately omit .withWatermark()
no_watermark = orders \
    .groupBy(window(col("created_at"), "5 minutes")) \
    .agg(_sum("amount").alias("total_revenue"))

query = no_watermark.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()
```

In `append` mode, Spark only emits a window when it knows the window is complete. Without a watermark, Spark never knows a window is complete. **No output will ever appear.** The pipeline runs silently, consuming memory for open windows, producing nothing. Eventually it'll OOM.

Students should observe this and confirm: no watermark + append mode = silent failure. This is why watermarks are not optional for windowed aggregations in production. In `update` mode without a watermark, you'll get output (partial updates), but state grows unboundedly. Either way, missing watermarks are a production incident waiting to happen.

### Final 20 minutes -- Synthesis discussion

Put the pieces together. Students should be able to answer:

**"Your business asks for revenue-per-minute, updated every 10 seconds, tolerating up to 2 minutes of late data. What are the parameters?"**

Answer: 1-minute tumbling window, 10-second trigger interval, 2-minute watermark. In `update` mode, downstream consumers see partial results every 10 seconds and final results ~2 minutes after the window closes. In `append` mode, results appear only after the 2-minute watermark passes.

**"What happens if Kafka consumer lag spikes to 5 minutes?"**

Answer: Spark processes a large backlog in the next micro-batch. The watermark advances based on event time in the data, not wall clock. If the events in the backlog span a wide event-time range, multiple windows may close at once. There's no data loss from lag alone -- the watermark is relative to the max event time seen, not wall time.

**"Your watermark drops 0.1% of events. Is that acceptable?"**

Answer: it depends on the business context. For a revenue dashboard, 0.1% might be fine. For regulatory reporting, it might not. The dropped events aren't gone from the source system -- they're still in Kafka, still in the OLTP database. You can always run a batch reconciliation job to catch them. This is a common production pattern: streaming for speed, batch for correctness. Students will see this pattern again in Lessons 10-11.

Remind them of where this fits in the arc: Lesson 5 captured the changes, Lesson 6 transported them durably, Lesson 7 computed over them in event time with lateness handling. Lesson 8 adds stateful operations (joins, deduplication). Lesson 9 asks whether Spark's micro-batch model is good enough or whether you need true continuous processing.

---

## Take-home deliverable

A GitHub repository containing:

- **Pipeline code** that consumes CDC events from Kafka and computes windowed revenue aggregations using PySpark Structured Streaming
- **Configurable watermark** -- the allowed lateness must be a parameter (CLI arg or config file), not hardcoded
- **Late-event demonstration** -- a script that injects late events and a recorded observation (in the README) of what happens with different watermark settings. Must include the completeness/latency table from Phase 3 with their own data
- **README.md** explaining how to run the pipeline, what parameters it accepts, and the tradeoff analysis for their chosen watermark setting
- **CLAUDE.md** (or AGENTS.md) documenting how AI assistance was used in developing the solution

Deliverable is submitted as a pull request. AI assistance is encouraged -- the goal is correct understanding of windowing semantics and watermark behavior, not typing speed.
