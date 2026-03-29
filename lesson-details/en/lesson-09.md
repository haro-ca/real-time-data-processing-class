# Lesson 9, Micro-batch vs. true streaming: Spark Structured Streaming vs. Flink

Students have built stream processing pipelines in Spark Structured Streaming (Lessons 7-8). They work. They produce correct results. But every time Spark runs a micro-batch, there's a floor on how fast results can appear, you can tune `trigger(processingTime="1 second")` or even `trigger(availableNow=True)`, but you can't escape the batch scheduling overhead. The pipeline from Lesson 7 computes 5-minute tumbling window aggregations over Kafka order events. It gives you results. The question this lesson answers is: **how late are those results, and does it matter?**

This is not a "Spark vs. Flink which is better" lesson. That framing is useless. This is a measurement lesson. Students will implement the same pipeline in both engines, instrument both with event timestamps, and produce latency CDFs that make the architectural difference visible. The recommendation at the end depends on the latency SLA, and for many real workloads, Spark's micro-batch is perfectly fine. Students need the data to know when it isn't.

## Hour 1, Theory: two execution models, one problem

### Module A, Spark's micro-batch model: what actually happens on each trigger

Start by opening the box on what Spark Structured Streaming has been doing in Lessons 7-8. Students used it; now they need to understand the execution model precisely enough to predict its latency behavior.

When you write `.trigger(processingTime="10 seconds")`, here's what happens every 10 seconds:

1. **Offset discovery.** Spark's driver queries Kafka for the latest offsets on each partition. This is a synchronous call to the Kafka broker, takes a few milliseconds typically, but it's on the critical path.
2. **Micro-batch planning.** The driver computes the offset range for each partition (last committed offset → current latest offset). This range defines the micro-batch. The driver creates a Spark SQL execution plan, a full DAG of operators, just like a batch query.
3. **Task scheduling.** The driver serializes the plan and ships it to executors. Each executor gets a task for a subset of partitions/offset ranges. This has overhead, JVM serialization, network transfer, task launch latency.
4. **Data processing.** Executors pull records from Kafka (using their own KafkaConsumer instances), deserialize, apply transformations, compute aggregations. This is the actual work.
5. **Output commit.** Results are written to the sink. Spark then atomically commits the new offsets to the checkpoint directory. Only after this commit does the micro-batch complete.
6. **Wait.** If the trigger interval hasn't elapsed, Spark waits until the next trigger. If processing took longer than the interval, the next batch starts immediately.

The latency floor is the sum of steps 1-5. Even if the trigger interval is 0 (the `processingTime="0 seconds"` trick, start the next batch immediately), you still pay the per-batch overhead: offset discovery, planning, scheduling, commit. On a healthy cluster, this overhead is typically 50-200ms per micro-batch even for trivial workloads. For complex jobs with shuffles, it's often 500ms-2s.

**Key insight to drive home:** micro-batch latency is not the trigger interval. It's `max(trigger_interval, batch_processing_time)`. A record that arrives at Kafka 1ms after a batch starts won't be processed until the next batch, so the worst-case latency is approximately `2 * trigger_interval + batch_processing_time`. Students should derive this on the board.

Mention Spark's experimental continuous processing mode (`trigger(continuous="1 second")`). It launched in Spark 2.3 with enormous fanfare and hasn't meaningfully advanced since. It supports only `map`-like operations, no aggregations, no joins, no windowing. For the pipeline students built in Lesson 7 (windowed aggregation), continuous mode is not applicable. Be blunt about this: it's a dead end for most real workloads. If students ask "why not just use continuous mode?", the answer is "because it can't do what you need it to do."

### Module B, Flink's true streaming model: processing records one at a time

Flink does not batch. When a record arrives at a Flink source, it is immediately deserialized, passed through the operator chain, and can produce output within milliseconds, not seconds. There is no planning phase, no task scheduling per record, no batch boundary.

The execution model:

1. **Source operators** run continuously, pulling records from Kafka partitions. Each source subtask owns a set of partitions (similar to Kafka consumer group assignment) and emits records downstream as they arrive.
2. **Records flow through the operator DAG.** Each operator (map, filter, keyBy, window) processes records as they arrive. State is maintained in the operator's local state backend (RocksDB or heap). There's no "collect a batch then process" step.
3. **Watermarks propagate through the DAG.** Sources periodically emit watermark events ("I believe no events with timestamp < W will arrive"). When a window operator receives a watermark that passes the window's end time, it fires the window and emits results. This is how Flink knows when a window is "complete" without waiting for a batch boundary.
4. **Output is emitted as it's produced.** When a window fires, the result is immediately sent to the sink operator. No commit phase per record, checkpointing handles durability asynchronously.

The latency floor is determined by: network transfer from Kafka, deserialization, operator processing, and watermark frequency. For a simple windowed aggregation, end-to-end latency from event production to result emission is typically 10-100ms, depending on watermark strategy. Compare that to Spark's 500ms-2s floor.

**But there's a cost.** Per-record processing means per-record overhead: function calls, state lookups, serialization of intermediate results. For very high-throughput workloads (millions of records/second), Spark's micro-batch approach can be more efficient, it amortizes overhead across the batch, uses columnar in-memory formats, and benefits from Spark SQL's whole-stage code generation. Flink processes each record individually, which is more overhead per record but less latency per record.

Draw the throughput-latency tradeoff on the board:

```
Throughput
    ^
    |     Spark micro-batch
    |    xxxxxxx
    |   x       x
    |  x         x
    | x            x
    |x    Flink      xxxxxxx
    |xxxxxxx                x
    +-----------------------------> Latency
    1ms   10ms  100ms   1s   10s
```

This is the core tradeoff: Spark trades latency for throughput efficiency. Flink trades throughput efficiency for latency. Neither is universally better. The question is always: **what is your latency SLA?**

### Module C, PyFlink DataStream API: the programming model

Students will use PyFlink to implement their pipeline. The DataStream API is Flink's lower-level stream processing interface, it gives you explicit control over keying, windowing, state, and time semantics.

Walk through the key abstractions:

**StreamExecutionEnvironment**, the entry point. Equivalent to Spark's `SparkSession`, but for Flink. You configure parallelism, checkpointing, and state backend here.

**DataStream**, a stream of records, typed. You apply transformations: `map()`, `filter()`, `key_by()`, `window()`, `reduce()`, `process()`. Unlike Spark where transformations build a DAG that executes in batches, Flink's transformations define a continuously running operator graph.

**key_by()**, partitions the stream by a key function, analogous to Spark's `groupBy()` but happening continuously. Records with the same key go to the same operator instance, which maintains its own state for that key.

**Windowing**, after `key_by()`, you attach a window assigner (tumbling, sliding, session) and a window function (reduce, aggregate, process). The window collects records, waits for the watermark to pass the window end, then fires.

**Watermarks**, Flink requires you to define a watermark strategy. The most common is `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))`, this tells Flink "events may arrive up to 5 seconds late; the watermark is the max observed event time minus 5 seconds." This directly controls the latency-completeness tradeoff from Lesson 7.

Show the skeleton of a PyFlink windowed aggregation. Don't run it yet, just establish the vocabulary:

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)

# Checkpoint every 10 seconds (async, non-blocking)
env.enable_checkpointing(10_000)

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka-1:9092")
    .set_topics("orders")
    .set_group_id("flink-pipeline")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(...)  # extract event time from JSON
)

ds = env.from_source(
    kafka_source,
    watermark_strategy,
    "kafka-orders"
)

# Parse, key, window, aggregate, covered in the practical
```

Point out the differences from Spark:

| Concept | Spark Structured Streaming | PyFlink DataStream |
|---|---|---|
| Entry point | `SparkSession` | `StreamExecutionEnvironment` |
| Source | `readStream.format("kafka")` | `KafkaSource.builder()` |
| Time semantics | Event time via `withWatermark()` on a column | `WatermarkStrategy` attached to the source |
| Windowing | `.groupBy(window(...))` on a DataFrame | `.key_by(...).window(TumblingEventTimeWindows.of(...))` |
| Execution | Micro-batch (batch planning per trigger) | Continuous (records flow through operators) |
| Checkpointing | Per-batch atomic commit to checkpoint dir | Periodic async snapshots via Chandy-Lamport |
| State backend | In-memory (default), RocksDB (optional) | HashMapStateBackend or RocksDB |

### Module D, Flink SQL: the high-level alternative

Flink also supports SQL for stream processing. The same pipeline you build with the DataStream API can often be written as a SQL query. Show this briefly, 10 minutes, no hands-on, to illustrate the tradeoff.

The same windowed aggregation in Flink SQL:

```sql
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka-1:9092',
    'properties.group.id' = 'flink-sql-pipeline',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(ts, INTERVAL '5' MINUTE) AS window_end,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM orders
GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE);
```

That's it. The Kafka connector, deserialization, watermark strategy, windowing, and aggregation, all declared in SQL. The Flink optimizer turns this into the same physical operator graph as the DataStream API.

**When to use SQL vs. DataStream:**

- **Flink SQL** is the right default for aggregations, joins, and standard analytics. It's less code, the optimizer handles parallelism and state management, and it's readable by analysts who don't write python.
- **DataStream API** is necessary when you need custom processing logic that SQL can't express: complex event processing (CEP), custom window triggers, side outputs, manual state management, async I/O to external systems. The Lesson 7 pipeline is simple enough for SQL, but students use the DataStream API for the practical because they need to understand what SQL hides.

**What you give up with SQL:** direct control over state access, custom watermark logic, side outputs, and the ability to implement non-standard windowing behavior. If your pipeline fits in SQL, use SQL. If it doesn't, you'll know, because you'll hit a wall trying to express it.

### Module E, When micro-batch is good enough (and when it isn't)

Close the theory hour with the decision framework. This is what the deliverable is really about, students will fill in the numbers from their own benchmarks.

**Micro-batch (Spark) is good enough when:**

- Your latency SLA is >= 1 second. Spark can comfortably hit 1-2 second end-to-end latency with aggressive trigger intervals.
- Your throughput requirements are very high (millions of events/second) and you can't afford per-record overhead.
- Your team already knows Spark. The operational cost of running two engines (Spark for batch, Flink for streaming) is non-trivial.
- You need strong integration with the Spark ecosystem (MLlib, Spark SQL for batch, Delta Lake).

**True streaming (Flink) is necessary when:**

- Your latency SLA is < 500ms. Spark physically cannot hit this consistently, the micro-batch overhead floor prevents it.
- You need per-event processing semantics: complex event processing, real-time fraud detection, session windowing with sub-second triggers.
- Your watermark and late-data handling needs are complex (multiple watermarks, custom triggers, side outputs for late data).
- You're building a pipeline that needs to react to individual events, not batches of events.

**The gray zone (500ms-2s) is where the decision gets hard** and where the benchmark data from the practical matters most. In this range, both engines can technically deliver, but Flink does it with less jitter and Spark does it with less operational overhead (if you're already running Spark). This is the zone where the right answer is "measure it on your workload."

---

## Hour 2, Practical: re-implement the Lesson 7 pipeline in PyFlink

### Setup (20 min)

Flink runs in Docker. Provide a `docker-compose.yml` that adds a Flink JobManager and TaskManager alongside the existing Kafka cluster from Lesson 6:

```yaml
  flink-jobmanager:
    image: flink:1.19-java11
    container_name: flink-jobmanager
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
        parallelism.default: 4
        state.backend: hashmap
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        execution.checkpointing.interval: 10s
    ports:
      - "8081:8081"  # Flink Web UI
    volumes:
      - ./flink-jobs:/opt/flink/jobs
      - flink-checkpoints:/tmp/flink-checkpoints

  flink-taskmanager:
    image: flink:1.19-java11
    container_name: flink-taskmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
        parallelism.default: 4
        state.backend: hashmap
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        execution.checkpointing.interval: 10s
    depends_on:
      - flink-jobmanager
    volumes:
      - ./flink-jobs:/opt/flink/jobs
      - flink-checkpoints:/tmp/flink-checkpoints

volumes:
  flink-checkpoints:
```

Students install PyFlink in their local environment: `pip install apache-flink==1.19.1`. This is a large install (~500MB) because it bundles a JVM and the Flink runtime. It takes a few minutes, start the download early. They also need the Flink Kafka connector JAR, which they can download and place in the Flink lib directory or pass via `--jarfile` when submitting. Provide the exact command:

```bash
# Download the Kafka connector JAR
wget -P ./flink-jobs/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.19/flink-sql-connector-kafka-3.1.0-1.19.jar
```

Students should also have their Spark pipeline from Lesson 7 running and functional. They'll need both engines running simultaneously during Phase 3.

Verify the Flink cluster is healthy by opening the Flink Web UI at `http://localhost:8081`. Students should see 1 TaskManager with 4 available task slots.

### Phase 1, PyFlink windowed aggregation pipeline (30 min)

Students re-implement their Lesson 7 pipeline: consume order events from Kafka, compute revenue per 5-minute tumbling window, output results. Same logic, different engine.

Here's the full implementation. Students should type this, not copy-paste, the muscle memory of the API differences matters:

```python
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.common.watermark_strategy import TimestampAssigner

logging.basicConfig(level=logging.INFO)


class OrderTimestampAssigner(TimestampAssigner):
    """Extract event time from the order's 'ts' field."""

    def extract_timestamp(self, value, record_timestamp):
        # value is a dict at this point (after parsing)
        # Return milliseconds since epoch
        from datetime import datetime

        dt = datetime.fromisoformat(value["ts"].replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)


class ParseOrder(MapFunction):
    """Parse JSON string into a dict."""

    def map(self, value):
        order = json.loads(value)
        return order


class WindowAggregation(ProcessWindowFunction):
    """Compute per-window aggregation: count, sum, avg."""

    def process(self, key, context, elements):
        orders = list(elements)
        count = len(orders)
        total = sum(o["amount"] for o in orders)
        avg = total / count if count > 0 else 0.0

        window = context.window()
        result = {
            "window_start": window.start,
            "window_end": window.end,
            "order_count": count,
            "total_revenue": round(total, 2),
            "avg_order_value": round(avg, 2),
            "emit_ts_ms": int(__import__("time").time() * 1000),
        }
        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(10_000)

    # Add the Kafka connector JAR
    env.add_jars("file:///opt/flink/jobs/lib/flink-sql-connector-kafka-3.1.0-1.19.jar")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka-1:9092")
        .set_topics("orders")
        .set_group_id("flink-windowed-agg")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    )

    ds = env.from_source(kafka_source, watermark_strategy, "kafka-orders")

    # Parse JSON → dict
    parsed = ds.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())

    # Assign timestamps from event data (after parsing)
    # We need to re-assign watermarks with the actual event timestamp
    timestamped = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(OrderTimestampAssigner())
    )

    # Key by a fixed key (aggregate all orders) or by customer_id
    # Using "all" key to match Lesson 7's total revenue window
    windowed = (
        timestamped
        .key_by(lambda x: "all", key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(WindowAggregation(), output_type=Types.STRING())
    )

    # Print to stdout for now (students can add a Kafka sink later)
    windowed.print()

    env.execute("flink-windowed-aggregation")


if __name__ == "__main__":
    main()
```

**Walk through the key differences from the Spark version as students build this:**

1. **No DataFrame abstraction.** Spark treats streams as infinite DataFrames with schema. PyFlink DataStream works with typed records, dicts, tuples, or custom objects. You manage serialization explicitly (note `Types.PICKLED_BYTE_ARRAY()` for python objects). This is more verbose but gives you more control.

2. **Watermark assignment is explicit and attached to the source.** In Spark, you call `.withWatermark("ts", "5 seconds")` on the DataFrame, the engine extracts timestamps from a column. In Flink, you provide a `TimestampAssigner` that the engine calls for every record. This means Flink's watermarks are set before any transformation, not after a column expression.

3. **Window processing is a function, not a SQL expression.** Spark's `.groupBy(window(col("ts"), "5 minutes")).agg(...)` is declarative. PyFlink's `.window(TumblingEventTimeWindows.of(Time.minutes(5))).process(WindowAggregation())` is imperative, you write a function that receives all records in the window and yields output. More power, more code.

4. **`emit_ts_ms` is the measurement hook.** Students record `time.time()` when the window fires. Combined with the window end time (which is derived from event time), this gives the processing latency: `emit_ts_ms - window_end`. This is how they'll build the latency CDF.

**Common PyFlink gotchas students will hit:**

- **JAR dependencies.** The Kafka connector JAR must be on Flink's classpath. If students see `ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource`, the JAR isn't loaded. The `env.add_jars()` call handles this, but the path must be correct.
- **python serialization overhead.** PyFlink uses a python-JVM bridge (Py4J) and serializes python objects via pickle. This is slower than native Java/Scala Flink. For this exercise it's fine, the bottleneck will be Kafka and windowing, not serialization. In production, Flink SQL or Java is typically used for latency-sensitive pipelines.
- **Watermark propagation with `PICKLED_BYTE_ARRAY`.** After using `map()` with `PICKLED_BYTE_ARRAY`, watermarks from the source may not propagate correctly because Flink can't extract timestamps from pickled bytes. This is why the code re-assigns watermarks with a `TimestampAssigner` after parsing. Students who skip this step will see windows never fire.

### Phase 2, Instrument both pipelines for latency measurement (20 min)

This is the most important phase. Without instrumentation, the lesson is just "run two things and look at the output." With instrumentation, it's science.

**The measurement protocol:**

Both pipelines need to record two timestamps per output record:

1. **Window end time** (event time), the logical time at which the window closes. This is the same for both engines given the same data.
2. **Emit timestamp** (wall clock), the physical time at which the result was produced. This differs between engines.

**Processing latency** = emit timestamp - window end time. This measures how long after the logical window close the engine actually produced the result. For Flink, this should be close to the watermark delay (5 seconds in the config above) plus processing time. For Spark, it should be the watermark delay plus the batch processing time plus the wait-for-next-trigger time.

Students must also record on the producer side:

3. **Production timestamp**, wall clock time when each event was sent to Kafka. Embed this in the event JSON as `produced_at_ms`.

This gives a second metric: **end-to-end latency** = emit timestamp - production timestamp of the last event in the window. This captures the full pipeline delay.

Add instrumentation to the Spark pipeline (students modify their Lesson 7 code):

```python
from pyspark.sql.functions import (
    window, col, count, sum as spark_sum, avg,
    current_timestamp, unix_timestamp,
    from_json,
)

# In the streaming query output, add emit timestamp
result = (
    orders
    .withWatermark("ts", "5 seconds")
    .groupBy(window(col("ts"), "5 minutes"))
    .agg(
        count("*").alias("order_count"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
    )
    .withColumn("emit_ts_ms", (unix_timestamp(current_timestamp()) * 1000).cast("long"))
)
```

Both pipelines write results to a shared location, a Kafka topic (`pipeline-results-spark` and `pipeline-results-flink`) or output files. Students will collect these results after the benchmark run and compute latency distributions.

Write a shared producer that generates events at a controlled rate with embedded production timestamps:

```python
import json
import time
from confluent_kafka import Producer
from datetime import datetime, timezone

producer = Producer({
    "bootstrap.servers": "localhost:19092",
    "acks": "all",
    "enable.idempotence": True,
})

def generate_orders(rate_per_second=100, duration_seconds=600):
    """Generate orders at a fixed rate for a fixed duration."""
    interval = 1.0 / rate_per_second
    start = time.time()

    order_id = 0
    while time.time() - start < duration_seconds:
        now_ms = int(time.time() * 1000)
        order = {
            "order_id": order_id,
            "customer_id": order_id % 50,
            "amount": round(10.0 + (order_id % 100) * 1.5, 2),
            "ts": datetime.now(timezone.utc).isoformat(),
            "produced_at_ms": now_ms,
        }
        producer.produce(
            topic="orders",
            key=str(order["customer_id"]).encode("utf-8"),
            value=json.dumps(order).encode("utf-8"),
        )
        producer.poll(0)
        order_id += 1
        time.sleep(interval)

    producer.flush()
    print(f"Produced {order_id} orders in {duration_seconds}s")
```

**Key instruction:** both pipelines must consume from the same Kafka topic with the same data. Run the producer first to load 10 minutes of data, then run both pipelines against that data. This ensures an apples-to-apples comparison. If students run the producer concurrently with the pipelines, timing differences in production rate will skew the comparison.

---

## Hour 3, Benchmark, measure, and decide

### Phase 3, Run the benchmark (20 min)

The protocol:

1. **Produce 10 minutes of order events** at 100 events/second to the `orders` topic. That's 60,000 events, enough to fill many 5-minute windows.
2. **Run the Spark pipeline** against the topic. Use `trigger(processingTime="2 seconds")`, this is aggressive but achievable. Record all output records with their `emit_ts_ms` and `window.end` timestamps. Write results to `pipeline-results-spark` topic or a local file.
3. **Run the Flink pipeline** against the same topic. Record all output records with their `emit_ts_ms` and `window_end` timestamps. Write results to `pipeline-results-flink` topic or a local file.
4. **Collect results** from both pipelines.

Students compute, for each window result:

```python
import json

# For each result record from either pipeline:
# processing_latency_ms = emit_ts_ms - window_end_ms
# end_to_end_latency_ms = emit_ts_ms - max(produced_at_ms for events in that window)
```

Then compute the CDF (cumulative distribution function) for both latency metrics, for both engines:

```python
import numpy as np

def compute_latency_cdf(latencies_ms):
    """Return (sorted_latencies, cdf_values) for plotting."""
    sorted_lat = np.sort(latencies_ms)
    cdf = np.arange(1, len(sorted_lat) + 1) / len(sorted_lat)
    return sorted_lat, cdf

# Example: plot both CDFs on the same axes
import matplotlib.pyplot as plt

spark_lat, spark_cdf = compute_latency_cdf(spark_processing_latencies)
flink_lat, flink_cdf = compute_latency_cdf(flink_processing_latencies)

plt.figure(figsize=(10, 6))
plt.plot(spark_lat, spark_cdf, label="Spark Structured Streaming", linewidth=2)
plt.plot(flink_lat, flink_cdf, label="PyFlink DataStream", linewidth=2)
plt.axhline(y=0.99, color="gray", linestyle="--", alpha=0.5, label="p99")
plt.xlabel("Processing Latency (ms)")
plt.ylabel("CDF")
plt.title("Processing Latency CDF: Spark vs. Flink")
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig("latency_cdf.png", dpi=150, bbox_inches="tight")
```

**Expected results (guide students if their numbers are wildly different):**

| Metric | Spark (trigger=2s) | PyFlink |
|---|---|---|
| p50 processing latency | 3-5 seconds | 5-6 seconds (watermark delay dominates) |
| p99 processing latency | 6-10 seconds | 5.5-7 seconds |
| Latency jitter (p99-p50) | 3-6 seconds | 0.5-1.5 seconds |

The headline finding: **Flink's latency distribution is tighter.** Spark's latency has high variance because of the batch boundary effect, records that arrive just after a batch starts wait for the full trigger interval plus the next batch's processing. Flink's latency is dominated by the watermark delay, which is consistent. The p99 gap might not be enormous (depending on the trigger interval), but the jitter difference is significant.

If students reduce Spark's trigger to `processingTime="500 milliseconds")`, the gap narrows, but the per-batch overhead becomes a larger fraction of the batch, and throughput efficiency drops. Have students try several trigger intervals and plot the throughput-latency curve.

### Phase 4, Vary the trigger interval (15 min)

Students run the Spark pipeline multiple times with different trigger intervals: 10s, 5s, 2s, 1s, 500ms. For each, record the p99 processing latency and the throughput (events/second processed). Plot these as a scatter plot:

```
p99 Latency (ms)
    ^
    |
15s |  x (trigger=10s)
    |
10s |      x (trigger=5s)
    |
 5s |          x (trigger=2s)
    |            x (trigger=1s)
 3s |              x (trigger=500ms)  <-- diminishing returns
    |                                      x Flink (no trigger)
    +-----------------------------------------> Throughput (events/s)
```

The curve shows diminishing returns: as trigger interval decreases below 2s, latency improves marginally but each batch pays a higher fraction of overhead for planning and scheduling. There's a floor below which Spark can't go, no matter how small the trigger. Flink sits below that floor.

**The question to ask:** at what trigger interval does Spark's overhead start eating into throughput? Students should see this in their data, when throughput drops as trigger interval decreases, they've found the overhead-dominated regime.

### Phase 5, The architectural recommendation (25 min)

This is the deliverable. Students write a recommendation for three latency SLA scenarios using their own benchmark data:

**Scenario 1: "Dashboard updates every 30 seconds"**, a business dashboard that refreshes every 30 seconds showing revenue per 5-minute window. Latency SLA: < 30 seconds.

Expected recommendation: **Spark Structured Streaming.** The latency SLA is easily met with a 10-second trigger interval. Spark's ecosystem integration (Delta Lake, MLlib, shared Spark cluster for batch and streaming) makes it the operationally simpler choice. Running Flink for this SLA is over-engineering.

**Scenario 2: "Real-time alerting with 5-second SLA"**, an alerting system that must detect anomalous order patterns within 5 seconds of occurrence. Latency SLA: p99 < 5 seconds.

Expected recommendation: **It depends.** Students should reference their CDF data. With an aggressive Spark trigger (500ms-1s), Spark might hit this SLA, but with higher jitter and less headroom. Flink hits it comfortably. The recommendation should weigh operational complexity (does the team already run Spark? is adding Flink worth it for this SLA?) against latency confidence.

**Scenario 3: "Fraud detection with 500ms SLA"**, a payment processing system that must flag suspicious transactions within 500ms. Latency SLA: p99 < 500ms.

Expected recommendation: **Flink (or not a windowed aggregation at all).** Spark cannot hit this SLA, the micro-batch overhead floor exceeds 500ms. Flink can, but only with aggressive watermark configuration and potentially event-at-a-time processing (no 5-minute windows, you'd use per-event CEP patterns instead). Students should note that this SLA might require rethinking the pipeline architecture entirely, not just swapping engines.

**The recommendation must reference specific data points from their CDFs.** "Spark can't hit 500ms" is not enough. "Our Spark p99 was 3.2 seconds at trigger=500ms, and reducing the trigger further showed diminishing returns, the per-batch overhead floor was approximately 400ms, leaving no room for actual processing within a 500ms SLA", that's the bar.

### Final 10 minutes, Honest discussion: what this benchmark doesn't show

Be upfront about the limitations of a single-machine Docker benchmark:

1. **PyFlink is slower than Java/Scala Flink.** The python-JVM bridge adds overhead. Production Flink pipelines that need sub-100ms latency are written in Java. The PyFlink numbers students got are a ceiling, not a floor, native Flink would be faster.

2. **Single-machine benchmarks hide network effects.** In a real cluster, Spark's micro-batch planning involves driver-executor communication. Flink's record-at-a-time model involves cross-node shuffles for `key_by()`. Both get slower with distribution.

3. **State size matters.** With 60,000 events and simple aggregations, state fits comfortably in memory. At millions of keys with complex state (like the stateful joins from Lesson 8), RocksDB spills to disk, and the performance characteristics change.

4. **Exactly-once overhead is different.** Spark's per-batch atomic commit is simple but coarse. Flink's Chandy-Lamport checkpointing is more sophisticated but can cause backpressure during checkpoints. At scale, checkpoint duration becomes a significant operational concern in Flink.

These caveats don't invalidate the benchmark, they frame it. The fundamental insight (micro-batch has a latency floor; true streaming doesn't) holds regardless of scale. The specific numbers change, but the shape of the CDF curves stays the same.

---

## Take-home deliverable

A GitHub repo submitted via PR containing:

**1. Code:**

- The PyFlink DataStream pipeline implementing the Lesson 7 windowed aggregation.
- The Spark Structured Streaming pipeline (ported from Lesson 7) with latency instrumentation.
- A shared producer that generates events with embedded production timestamps.
- A benchmarking script that runs both pipelines, collects results, and computes latency CDFs.
- Docker Compose file that includes Kafka + Flink (Spark assumed already running from L7-8).

**2. Benchmark report (in the README.md):**

- Latency CDF plots for both engines (processing latency and end-to-end latency).
- A table of p50, p95, and p99 latencies for both engines.
- The throughput-latency curve for Spark at different trigger intervals.
- Architectural recommendations for the three SLA scenarios, referencing specific data points from the benchmarks.

**3. AGENTS.md (or CLAUDE.md):**

- Project structure and how to run everything.
- How to reproduce the benchmark (exact commands, expected runtime).
- Key configuration decisions (why this watermark delay, why this trigger interval, why this parallelism).
- Interpretation guide: what the CDF plots show and how to read them.

AI assistance is encouraged. The evaluation is on measurement rigor, are the CDFs computed correctly, do the recommendations follow from the data, and can the student explain why Spark's latency distribution looks the way it does? Parroting "Flink is faster" without the CDF evidence is a failing grade. Recommending Flink for a 30-second SLA without justification is also a failing grade. The point is matching the tool to the requirement, backed by data.
