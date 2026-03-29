# Lesson 10, Real-time OLAP: serving the results

Lessons 1-3 established the OLTP/OLAP duality. Lessons 4-9 built the pipeline to move data between them, CDC, Kafka, stream processing. Now the question changes: **where do the analytical results live, and how do you serve them fast enough that a dashboard user doesn't notice the data is 2 seconds old?**

DuckDB is excellent for analytical queries. But DuckDB is an embedded engine, it reads files, it runs in-process, and it has no concept of concurrent ingestion while serving queries. If your pipeline produces a Parquet file every 5 minutes and DuckDB reads it, your dashboard is 5 minutes stale. For many use cases that's fine. For the use cases that drove you to build a streaming pipeline in the first place, it's not.

Real-time OLAP engines, ClickHouse, Apache Pinot, Apache Druid, solve a specific problem: **continuous ingestion of streaming data with sub-second analytical query latency, simultaneously.** This lesson makes that concrete.

## Hour 1, Theory: the architecture of real-time OLAP

### Module A, Pre-aggregation vs. on-the-fly computation

There are two ways to serve analytical results quickly:

**Pre-aggregation:** compute the answer before the query arrives. Materialized views, rollup tables, OLAP cubes. The dashboard query becomes a point lookup into a precomputed result. Latency: microseconds. The cost: you must anticipate every question. If the user asks something you didn't pre-aggregate, you either return nothing or fall back to a full scan.

**On-the-fly computation:** store the raw (or lightly transformed) events and compute aggregations at query time. The dashboard can ask any question. The cost: you need an engine fast enough to scan millions of rows in milliseconds.

Real systems use both. ClickHouse materialized views pre-aggregate hot paths (e.g., "revenue per minute per region" that powers the main dashboard). Ad-hoc drill-downs query raw data on the fly. The architecture decision is which queries justify pre-aggregation and which can tolerate a full scan.

**Key insight:** pre-aggregation is a space-time tradeoff with an operational tax. Every materialized view is another thing that can break, drift out of sync, or silently return stale data. The faster your OLAP engine is at raw scans, the less pre-aggregation you need, and the simpler your system becomes.

### Module B, Materialized views in ClickHouse

ClickHouse materialized views are not what Postgres calls materialized views. In Postgres, a materialized view is a cached query result you `REFRESH` manually. In ClickHouse, a materialized view is a **trigger on INSERT**, when data arrives in the source table, the materialized view's query runs on the new rows and writes the result to a destination table. It's incremental, continuous, and automatic.

```sql
-- Source table: raw events
CREATE TABLE events (
    event_time DateTime,
    user_id UInt32,
    event_type String,
    amount Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (event_time, user_id);

-- Destination table: pre-aggregated per-minute revenue
CREATE TABLE revenue_per_minute (
    minute DateTime,
    total_revenue Decimal(38, 2),
    event_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY minute;

-- Materialized view: the bridge
CREATE MATERIALIZED VIEW revenue_per_minute_mv
TO revenue_per_minute
AS SELECT
    toStartOfMinute(event_time) AS minute,
    sum(amount) AS total_revenue,
    count() AS event_count
FROM events
GROUP BY minute;
```

When you insert into `events`, ClickHouse automatically aggregates and inserts into `revenue_per_minute`. The `SummingMergeTree` engine merges rows with the same `minute` key by summing `total_revenue` and `event_count` during background merges.

There's a subtlety students must understand: the materialized view processes each inserted **block** independently. If you insert 1000 events in one batch, the MV runs its GROUP BY on those 1000 events and writes partial aggregates. Later, the `SummingMergeTree` merges those partial aggregates. Until merging completes, a `SELECT` might return multiple rows for the same minute. To get the correct result, you must query with a final aggregation:

```sql
SELECT minute, sum(total_revenue), sum(event_count)
FROM revenue_per_minute
GROUP BY minute
ORDER BY minute;
```

Or use `SELECT ... FROM revenue_per_minute FINAL`, which forces merge-on-read at query time (slower, but correct without the outer aggregation). This is a gotcha that trips up every ClickHouse beginner and matters in production.

### Module C, How real-time OLAP engines differ from batch OLAP

DuckDB and Snowflake are excellent at analytics on static data. They assume the data is loaded, then queried. Their architecture reflects this:

- **DuckDB**: reads Parquet/CSV files or its own database file. Single-writer. No server mode (the server mode is experimental and not production-ready as of 2025). Perfect for analysts on laptops. Not designed for continuous ingestion from Kafka.
- **Snowflake**: separates storage and compute. Queries spin up warehouse clusters on demand. Great for batch analytics. Micro-partition pruning is powerful. But ingestion goes through staging, which adds seconds to minutes of latency.

Real-time OLAP engines are architecturally different because they optimize for a workload that batch engines don't target: **concurrent reads and writes with low latency on both paths.**

| Dimension | DuckDB / Snowflake (batch) | ClickHouse / Pinot / Druid (real-time) |
|---|---|---|
| Ingestion model | Load files, then query | Continuous streaming ingestion |
| Ingestion latency | Seconds to minutes | Sub-second to low seconds |
| Query during ingestion | Not designed for it | First-class requirement |
| Concurrency model | Single-writer (DuckDB) or isolated warehouses (Snowflake) | Concurrent readers and writers |
| Deployment | Embedded (DuckDB) or managed service (Snowflake) | Server process, cluster-capable |

The three major real-time OLAP engines each have a distinct personality:

- **ClickHouse**: column-oriented, MergeTree storage engine, SQL-native. Extremely fast on raw scans. The most "database-like" of the three, it feels like a fast Postgres for analytics. Strongest at ad-hoc analytical queries. Written in C++, obsessively optimized.
- **Apache Druid**: designed for time-series at scale. Pre-ingestion rollup is core to its architecture, it can aggregate at ingestion time, reducing storage. Segment-based storage with deep integration into the Kafka/HDFS ecosystem. Best when you know your query patterns upfront and want to push aggregation as early as possible.
- **Apache Pinot**: LinkedIn's real-time analytics engine. Similar to Druid architecturally but with a stronger focus on user-facing analytics (high QPS, low latency). Star-tree indexes for pre-aggregated queries. Best when you need to serve thousands of concurrent dashboard users.

We use ClickHouse in this lesson because it's the most general-purpose and has the lowest barrier to a meaningful practical exercise. It speaks SQL, runs in a single Docker container, and has native Kafka integration.

### Module D, LSM trees in an OLAP context

Students have seen LSM trees if they've touched RocksDB (Lesson 8's state backend). In OLAP, the same principle appears with a different accent.

ClickHouse's MergeTree engine works like this:

1. **Incoming data** is written to an in-memory buffer (or directly to a small "part" on disk).
2. Each **part** is a self-contained unit: column files, a primary index (sparse, one entry per 8192 rows by default, called a "granule"), and metadata. Parts are immutable once written.
3. A background process continuously **merges** smaller parts into larger ones, compacting, re-sorting by the `ORDER BY` key, and applying more aggressive compression.

This is an LSM-tree variant. The write path is fast because it's append-only, no random I/O, no rewriting existing data. The read path must potentially scan multiple parts (each with its own index), but merging keeps the number of parts manageable.

The merge process is also where `SummingMergeTree`, `AggregatingMergeTree`, and `ReplacingMergeTree` do their work, they apply custom merge logic (summing, aggregating, deduplicating) during compaction. This is why ClickHouse's specialized engines exist: they encode domain-specific merge semantics into the storage layer.

**The tradeoff:** write amplification. Each row is written once, then re-written during each merge level. A row might be physically written 3-5 times before it reaches its final merged state. This is fine for OLAP, you're optimizing for read throughput, not write efficiency. But it means ClickHouse needs more disk bandwidth than a naive "data size / throughput" calculation suggests.

### Module E, Ingestion latency vs. query latency: the fundamental tradeoff

This is the conceptual core of the lesson.

**Ingestion latency:** how long after an event occurs before it's queryable. In ClickHouse, data inserted via `INSERT` is queryable immediately (it's in a part on disk). Data from Kafka via the Kafka engine is queryable after the next poll-and-flush cycle, typically 1-5 seconds.

**Query latency:** how long a query takes to return. This depends on how much data it scans, how many parts exist (more parts = more overhead per query), and whether a materialized view pre-aggregated the answer.

The tradeoff: if you flush Kafka data more frequently (smaller batches, lower ingestion latency), you create more small parts. More small parts means more work per query (each part has index lookup overhead) until background merges compact them. Aggressive merging consumes CPU and disk I/O, which competes with query execution.

In practice, you tune this with three levers:

1. **`kafka_max_block_size`**: how many rows to buffer from Kafka before flushing to a part. Larger = fewer parts, higher ingestion latency.
2. **`max_insert_block_size`**: similar lever for direct inserts.
3. **Merge aggressiveness** (`merge_tree` settings): how quickly the background merger runs. More aggressive = fewer parts at steady state, but more CPU/IO contention.

For a dashboard that refreshes every 5 seconds, an ingestion latency of 2-3 seconds is fine. For a fraud detection alert that needs sub-second freshness, you push smaller batches and accept more query overhead. Students will tune this in the practical.

---

## Hour 2, Practical: ClickHouse + Kafka + FastAPI

### Setup (15 min)

Provide a Docker Compose file that brings up:

- **ClickHouse** (single node, `clickhouse/clickhouse-server` image)
- **Kafka** (reuse from Lessons 6-9, KRaft mode)
- **A python service container** (or students run locally with a venv)

Students should already have a Kafka topic with streaming events from previous lessons. If not, provide a simple python producer that generates synthetic e-commerce events:

```python
# event_producer.py, generates synthetic events for students who
# don't have the pipeline from earlier lessons running
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)
topic = "events"

regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
event_types = ["purchase", "refund", "page_view", "add_to_cart"]

while True:
    event = {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 100_000),
        "event_type": random.choice(event_types),
        "amount": round(random.uniform(1.0, 500.0), 2) if random.random() > 0.3 else 0,
        "region": random.choice(regions),
    }
    producer.produce(topic, json.dumps(event).encode("utf-8"))
    producer.poll(0)
    time.sleep(0.001)  # ~1000 events/sec
```

Run this in the background. Students should verify events are flowing with `kafka-console-consumer`.

### Phase 1, ClickHouse tables and Kafka ingestion (25 min)

Students create the ClickHouse tables. This is where they wire the Kafka stream into ClickHouse.

**Step 1: Create the destination table.**

```sql
CREATE TABLE events (
    event_time DateTime64(3),
    user_id UInt32,
    event_type LowCardinality(String),
    amount Decimal(10, 2),
    region LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_type, region, event_time)
SETTINGS index_granularity = 8192;
```

Walk through the design decisions:

- **`LowCardinality(String)`**: ClickHouse's dictionary encoding. For columns with few distinct values (`event_type` has 4, `region` has 5), this reduces storage and speeds up GROUP BY dramatically. This is the equivalent of what DuckDB does automatically, in ClickHouse you declare it.
- **`ORDER BY (event_type, region, event_time)`**: this determines the physical sort order on disk and the sparse primary index. Queries that filter on `event_type` or `region` first will benefit from index granule skipping. If students mostly query by time range, `ORDER BY (event_time, event_type, region)` would be better. The choice depends on the query pattern, make them think about this.
- **`PARTITION BY toYYYYMMDD(event_time)`**: each day gets its own partition (set of parts). Dropping old data is a partition drop, not a DELETE. Queries filtered to a single day skip all other partitions entirely.

**Step 2: Create the Kafka engine table.**

```sql
CREATE TABLE events_kafka (
    event_time DateTime64(3),
    user_id UInt32,
    event_type String,
    amount Decimal(10, 2),
    region String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536,
    kafka_poll_timeout_ms = 1000;
```

The Kafka engine table is not a real table, you don't query it directly. It's a consumer that pulls from Kafka. Data flows through it but isn't stored here.

**Step 3: Create the materialized view that bridges them.**

```sql
CREATE MATERIALIZED VIEW events_kafka_mv TO events AS
SELECT
    parseDateTimeBestEffort(event_time) AS event_time,
    user_id,
    event_type,
    amount,
    region
FROM events_kafka;
```

When ClickHouse polls Kafka and gets a batch of messages, the materialized view transforms them and inserts into the `events` table. This is the ingestion pipeline, no external consumer code needed.

Students should verify data is flowing:

```sql
SELECT count() FROM events;
-- Run twice, 5 seconds apart. Count should increase.

SELECT * FROM events ORDER BY event_time DESC LIMIT 5;
```

**Step 4: Create a pre-aggregated materialized view for the dashboard's hot query.**

```sql
CREATE TABLE revenue_by_region_minute (
    minute DateTime,
    region LowCardinality(String),
    total_revenue Decimal(38, 2),
    purchase_count UInt64,
    unique_users AggregateFunction(uniq, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (region, minute);

CREATE MATERIALIZED VIEW revenue_by_region_minute_mv TO revenue_by_region_minute AS
SELECT
    toStartOfMinute(event_time) AS minute,
    region,
    sumState(amount) AS total_revenue,
    countState() AS purchase_count,
    uniqState(user_id) AS unique_users
FROM events
WHERE event_type = 'purchase'
GROUP BY minute, region;
```

Note the `-State` and `-Merge` suffix pattern. `sumState()` stores a partial aggregate that `sumMerge()` combines at query time. `uniqState()` stores HyperLogLog sketches that `uniqMerge()` combines. This is how ClickHouse handles the "partial aggregates from different insert batches" problem correctly, it stores intermediate state, not final values.

Query the pre-aggregated table:

```sql
SELECT
    minute,
    region,
    sumMerge(total_revenue) AS revenue,
    countMerge(purchase_count) AS purchases,
    uniqMerge(unique_users) AS unique_users
FROM revenue_by_region_minute
WHERE minute >= now() - INTERVAL 10 MINUTE
GROUP BY minute, region
ORDER BY minute DESC, region;
```

### Phase 2, Build the dashboard query layer (25 min)

The instructor provides a FastAPI skeleton with the HTTP endpoints, error handling, and CORS already wired. Students fill in the ClickHouse queries and connection logic. They should not be spending time on API boilerplate, the learning objective is the ClickHouse query layer, not FastAPI.

**Instructor-provided skeleton** (students receive this):

```python
# app.py, FastAPI dashboard backend
# Students: implement the three functions marked with TODO
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import clickhouse_connect

client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = clickhouse_connect.get_client(host="localhost", port=8123)
    yield
    client.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])


@app.get("/api/revenue")
def revenue_by_region(minutes: int = Query(default=10, le=1440)):
    """Revenue per region for the last N minutes.
    TODO: implement using the pre-aggregated materialized view."""
    start = time.perf_counter()

    # --- STUDENT CODE START ---
    result = _query_revenue_by_region(minutes)
    # --- STUDENT CODE END ---

    elapsed_ms = (time.perf_counter() - start) * 1000
    return {"data": result, "query_ms": round(elapsed_ms, 2)}


@app.get("/api/throughput")
def event_throughput(minutes: int = Query(default=10, le=1440)):
    """Events per second over the last N minutes.
    TODO: implement by querying the raw events table."""
    start = time.perf_counter()

    # --- STUDENT CODE START ---
    result = _query_throughput(minutes)
    # --- STUDENT CODE END ---

    elapsed_ms = (time.perf_counter() - start) * 1000
    return {"data": result, "query_ms": round(elapsed_ms, 2)}


@app.get("/api/top-users")
def top_users(minutes: int = Query(default=10, le=1440), limit: int = Query(default=10, le=100)):
    """Top users by spend in the last N minutes.
    TODO: implement by querying the raw events table (no pre-aggregation)."""
    start = time.perf_counter()

    # --- STUDENT CODE START ---
    result = _query_top_users(minutes, limit)
    # --- STUDENT CODE END ---

    elapsed_ms = (time.perf_counter() - start) * 1000
    return {"data": result, "query_ms": round(elapsed_ms, 2)}
```

**What students implement:**

```python
def _query_revenue_by_region(minutes: int) -> list[dict]:
    """Query the pre-aggregated materialized view."""
    query = """
        SELECT
            minute,
            region,
            sumMerge(total_revenue) AS revenue,
            countMerge(purchase_count) AS purchases,
            uniqMerge(unique_users) AS unique_users
        FROM revenue_by_region_minute
        WHERE minute >= now() - toIntervalMinute({minutes:UInt32})
        GROUP BY minute, region
        ORDER BY minute DESC, region
    """
    result = client.query(query, parameters={"minutes": minutes})
    return [
        {
            "minute": str(row[0]),
            "region": row[1],
            "revenue": float(row[2]),
            "purchases": row[3],
            "unique_users": row[4],
        }
        for row in result.result_rows
    ]


def _query_throughput(minutes: int) -> list[dict]:
    """Query raw events table, no pre-aggregation."""
    query = """
        SELECT
            toStartOfMinute(event_time) AS minute,
            count() / 60 AS events_per_second
        FROM events
        WHERE event_time >= now() - toIntervalMinute({minutes:UInt32})
        GROUP BY minute
        ORDER BY minute
    """
    result = client.query(query, parameters={"minutes": minutes})
    return [
        {"minute": str(row[0]), "events_per_second": round(float(row[1]), 2)}
        for row in result.result_rows
    ]


def _query_top_users(minutes: int, limit: int) -> list[dict]:
    """Ad-hoc query on raw events, demonstrates on-the-fly aggregation."""
    query = """
        SELECT
            user_id,
            sum(amount) AS total_spend,
            count() AS purchase_count
        FROM events
        WHERE event_type = 'purchase'
          AND event_time >= now() - toIntervalMinute({minutes:UInt32})
        GROUP BY user_id
        ORDER BY total_spend DESC
        LIMIT {limit:UInt32}
    """
    result = client.query(query, parameters={"minutes": minutes, "limit": limit})
    return [
        {"user_id": row[0], "total_spend": float(row[1]), "purchase_count": row[2]}
        for row in result.result_rows
    ]
```

Students should run the API (`uvicorn app:app --reload`) and hit the endpoints with `curl`:

```bash
curl "http://localhost:8000/api/revenue?minutes=5"
curl "http://localhost:8000/api/throughput?minutes=10"
curl "http://localhost:8000/api/top-users?minutes=5&limit=10"
```

Every response includes `query_ms`. Students should observe:

- `/api/revenue` (pre-aggregated): single-digit milliseconds. The materialized view already did the work.
- `/api/throughput` (raw scan, narrow time range): low tens of milliseconds. ClickHouse is scanning raw events but the columnar engine is fast enough.
- `/api/top-users` (raw scan with high-cardinality GROUP BY): tens of milliseconds, potentially more as data grows.

**Key observation:** all three endpoints return sub-second while the Kafka consumer is continuously inserting data. This is the point, concurrent ingestion and querying, both fast.

### Phase 3, Compare against DuckDB batch-loaded (20 min)

This is where the real-time OLAP value proposition becomes concrete. Students export the last 10 minutes of events from ClickHouse to a Parquet file, then query it in DuckDB:

```python
# export_and_compare.py
import time
import duckdb
import clickhouse_connect

ch = clickhouse_connect.get_client(host="localhost", port=8123)

# Export recent data to Parquet
print("Exporting from ClickHouse to Parquet...")
result = ch.query("SELECT * FROM events WHERE event_time >= now() - INTERVAL 10 MINUTE")
# Write to Parquet via DuckDB (the irony is intentional)
duck = duckdb.connect()
duck.execute("CREATE TABLE events AS SELECT * FROM result.result_rows")  # simplified
# In practice, students should write a proper export, the point is the workflow

# Now query DuckDB
start = time.perf_counter()
duck.sql("""
    SELECT
        date_trunc('minute', event_time) AS minute,
        region,
        sum(amount) AS revenue,
        count(*) AS purchases
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY minute, region
    ORDER BY minute DESC, region
""")
duckdb_ms = (time.perf_counter() - start) * 1000
print(f"DuckDB query: {duckdb_ms:.1f} ms")

# Same query against ClickHouse raw table (not materialized view)
start = time.perf_counter()
ch.query("""
    SELECT
        toStartOfMinute(event_time) AS minute,
        region,
        sum(amount) AS revenue,
        count() AS purchases
    FROM events
    WHERE event_type = 'purchase'
      AND event_time >= now() - INTERVAL 10 MINUTE
    GROUP BY minute, region
    ORDER BY minute DESC, region
""")
ch_ms = (time.perf_counter() - start) * 1000
print(f"ClickHouse query: {ch_ms:.1f} ms")
```

**Expected result on a small dataset (10 minutes, ~600k events):** DuckDB and ClickHouse raw query times will be comparable, both are columnar engines, both are fast. DuckDB might even be faster on a small in-memory dataset because it has zero network overhead (in-process).

This is the honest answer: **for small, static datasets, DuckDB wins on simplicity.** The difference emerges when students consider:

1. **The export step itself took time.** That's your ingestion latency in a batch workflow, you're always querying stale data.
2. **The export is a point-in-time snapshot.** Events that arrived during the export aren't included. In ClickHouse, they're queryable within seconds.
3. **At scale (billions of rows, hundreds of concurrent queries), DuckDB's single-process model doesn't serve a dashboard.** ClickHouse handles concurrent queries as a server.

Students should write a paragraph in their deliverable articulating this tradeoff: when DuckDB is the right choice (ad-hoc analysis, small data, single user) vs. when a real-time OLAP engine is justified (continuous ingestion, concurrent users, freshness SLA).

---

## Hour 3, Measure, tune, and stress-test

### Experiment A, Measure ingestion latency (15 min)

How fresh is the data? Students measure the gap between event generation and queryability:

```python
# measure_freshness.py
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import clickhouse_connect

producer = Producer({"bootstrap.servers": "localhost:9092"})
ch = clickhouse_connect.get_client(host="localhost", port=8123)

# Produce a canary event with a known timestamp
canary_time = datetime.now(timezone.utc)
canary_user_id = 999999999  # unlikely to collide
event = {
    "event_time": canary_time.isoformat(),
    "user_id": canary_user_id,
    "event_type": "purchase",
    "amount": 0.01,
    "region": "canary",
}
producer.produce("events", json.dumps(event).encode("utf-8"))
producer.flush()

# Poll ClickHouse until the canary appears
while True:
    result = ch.query(
        "SELECT count() FROM events WHERE user_id = {uid:UInt32}",
        parameters={"uid": canary_user_id},
    )
    if result.result_rows[0][0] > 0:
        latency = (datetime.now(timezone.utc) - canary_time).total_seconds()
        print(f"Ingestion latency: {latency:.2f} seconds")
        break
    time.sleep(0.1)
```

Run this 10 times. Report the distribution. Typical result: 1-5 seconds depending on `kafka_poll_timeout_ms` and `kafka_max_block_size` settings in the ClickHouse Kafka engine.

Then tune: decrease `kafka_max_block_size` to 1000 and re-measure. Latency should drop, but students should also check the number of parts being created (`SELECT count() FROM system.parts WHERE table = 'events' AND active`). More frequent flushes = more parts = more merge work.

### Experiment B, Query latency under concurrent ingestion (20 min)

The real test: does query latency degrade while ingestion is running at full speed?

Students write a load test that hammers the FastAPI endpoints while the Kafka producer is running:

```python
# query_loadtest.py
import asyncio
import time
import httpx

async def query_loop(client: httpx.AsyncClient, url: str, results: list):
    for _ in range(100):
        start = time.perf_counter()
        resp = await client.get(url)
        elapsed_ms = (time.perf_counter() - start) * 1000
        results.append(elapsed_ms)
        data = resp.json()
        # Also capture ClickHouse-side query time
        results.append(("server_ms", data.get("query_ms")))

async def main():
    latencies = []
    async with httpx.AsyncClient() as client:
        tasks = [
            query_loop(client, "http://localhost:8000/api/revenue?minutes=5", latencies)
            for _ in range(10)  # 10 concurrent query clients
        ]
        await asyncio.gather(*tasks)

    # Filter to just the float latencies (total round-trip)
    times = [t for t in latencies if isinstance(t, float)]
    times.sort()
    print(f"p50: {times[len(times)//2]:.1f} ms")
    print(f"p95: {times[int(len(times)*0.95)]:.1f} ms")
    print(f"p99: {times[int(len(times)*0.99)]:.1f} ms")

asyncio.run(main())
```

Students run this with the producer at ~1000 events/sec (baseline) and then crank the producer to 10,000 events/sec (remove the `sleep` or reduce it). Does query latency change? It shouldn't, meaningfully, ClickHouse's MergeTree isolates reads and writes well at this scale.

If query latency *does* degrade, the likely cause is excessive part creation overwhelming the merge scheduler. Students should check `system.merges` and `system.parts` to diagnose.

### Experiment C, The ORDER BY key matters (15 min)

Create a second table with a different sort order:

```sql
CREATE TABLE events_by_user (
    event_time DateTime64(3),
    user_id UInt32,
    event_type LowCardinality(String),
    amount Decimal(10, 2),
    region LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (user_id, event_time);

INSERT INTO events_by_user SELECT * FROM events;
```

Now compare query performance:

```sql
-- Query 1: filter by time range (favors events table, ORDER BY starts with event_type/region/event_time)
SELECT count(), sum(amount)
FROM events
WHERE event_time >= now() - INTERVAL 5 MINUTE;

SELECT count(), sum(amount)
FROM events_by_user
WHERE event_time >= now() - INTERVAL 5 MINUTE;

-- Query 2: filter by user (favors events_by_user table)
SELECT count(), sum(amount)
FROM events
WHERE user_id = 42;

SELECT count(), sum(amount)
FROM events_by_user
WHERE user_id = 42;
```

Use `EXPLAIN indexes = 1` to see how many granules are scanned vs. skipped. The table whose `ORDER BY` matches the query filter will skip dramatically more granules.

**Key insight:** the `ORDER BY` key in ClickHouse is not just a sort preference, it's the primary index. Choosing it wrong can mean the difference between scanning 100 granules and 100,000. This is the ClickHouse equivalent of choosing the right index in Postgres, but it's a table-level decision, not an afterthought. You pick it once, based on your dominant query pattern.

### Final 10 minutes, Synthesis discussion

Put up the comparison:

| Dimension | DuckDB (batch) | ClickHouse (real-time) |
|---|---|---|
| Data freshness | Minutes (batch export interval) | Seconds (Kafka → MergeTree) |
| Query latency (small range) | Sub-ms (in-process) | Low ms (network + query) |
| Concurrent query + ingest | Not designed for it | First-class |
| Operational complexity | Zero (embedded) | Medium (server, Kafka integration, tuning) |
| When to choose | Ad-hoc analysis, notebooks, CI tests | Live dashboards, user-facing analytics |

The key question for students: **what is the minimum data freshness SLA that justifies deploying a real-time OLAP engine instead of batch-loading DuckDB?**

There's no universal answer. If your dashboard updates every 15 minutes and has one viewer, DuckDB with a cron job is simpler and cheaper. If your dashboard updates every 5 seconds and serves 50 concurrent users, you need a real-time OLAP engine. The boundary is somewhere in between, and it depends on operational maturity as much as technical requirements.

The broader point: every component in this pipeline, CDC, Kafka, stream processing, real-time OLAP, adds operational cost. The course teaches you how to build all of it so you can make an informed decision about how much of it you actually need.

---

## Take-home deliverable

A GitHub repository containing:

- **Working code**: ClickHouse table definitions, Kafka ingestion setup, FastAPI query layer, comparison script
- **`README.md`**: setup instructions (Docker Compose up, run producer, hit endpoints), architecture diagram (text is fine), and a written analysis covering:
  - Measured ingestion latency (canary test results, distribution across 10+ runs)
  - Query latency under concurrent ingestion (p50/p95/p99 from the load test)
  - The `ORDER BY` key experiment results with `EXPLAIN` output
  - A comparison paragraph: when to use ClickHouse vs. DuckDB for their specific workload, with data to back it up
- **`AGENTS.md` or `CLAUDE.md`**: instructions for an AI coding assistant to understand the project structure, run tests, and extend the query layer. This is practice for the real world, projects should be legible to both humans and AI agents.

Submit via pull request. AI assistance is encouraged, the deliverable is the analysis and the working system, not proof that you typed every character yourself.
