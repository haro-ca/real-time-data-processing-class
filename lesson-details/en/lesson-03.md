# Lesson 3, Why is OLAP a fundamentally different problem?

Lesson 1 showed the ceiling of a single OLTP node. Lesson 2 showed the cost of distributing it. Both lessons were about small, fast transactions, point reads and writes. Now flip the workload entirely: instead of "insert one row as fast as possible," the question becomes "scan 100 million rows and answer a question." Same hardware. Completely different bottleneck profile. Students need to feel the difference before they can reason about it.

## Hour 1, Theory: why row stores are the wrong tool for analytical queries

### Module A, The physical layout problem

Start with a concrete scenario. You have a table with 100M taxi rides, pickup time, dropoff time, passenger count, trip distance, fare amount, tip amount, payment type, and 15 other columns. You want to answer: *"What was the average fare amount per month in 2023?"*

In Postgres (row store), data is laid out as heap tuples. Each 8KB page contains complete rows, all columns, packed sequentially. To compute the average fare, Postgres must:

1. Read every 8KB page of the table from disk (or buffer pool).
2. For each tuple, extract the `fare_amount` field (skipping past `pickup_time`, `dropoff_time`, `passenger_count`, `trip_distance`, bytes the query doesn't need).
3. Filter rows where `pickup_time` falls in 2023.
4. Accumulate the sum and count.

**The waste is enormous.** If the table has 20 columns and you need 2 of them, you're reading 10x more data than necessary. At 100M rows with ~200 bytes per row, that's ~20GB of I/O to answer a query that touches maybe 2GB of relevant data.

Now contrast with a column store. In a columnar layout, each column is stored contiguously, all 100M `fare_amount` values packed together, all 100M `pickup_time` values packed together, and so on. To answer the same query, the engine reads only the `fare_amount` and `pickup_time` columns. The other 18 columns are never touched. That's the I/O reduction right there, you read exactly what you need.

But it gets better. Contiguous values of the same type compress dramatically:

- A column of `payment_type` with 4 distinct values? **Dictionary encoding**, store a 2-bit code per row instead of the string.
- A column of sequential timestamps? **Delta encoding**, store the first value and then differences.
- A column of `passenger_count` where 80% of rides have 1 passenger? **Run-length encoding**, "1 repeated 847 times, 2 repeated 31 times, ..."

Compression ratios of 5-10x on columnar data are typical. So the 2GB of relevant column data might compress to 300MB on disk. Compare that to Postgres reading 20GB. **This is why the same hardware can scan billions of rows analytically but struggles with OLTP, the data layout changes the I/O profile by orders of magnitude.**

Drive this home with a napkin calculation:

| | Row store (Postgres) | Column store (DuckDB) |
|---|---|---|
| Data read from disk | ~20 GB (all columns) | ~300 MB (2 columns, compressed) |
| Disk bandwidth at 3 GB/s (NVMe) | ~6.7 seconds | ~0.1 seconds |

That's a 67x difference from layout and compression alone, before any execution engine optimizations.

### Module B, Vectorized execution

Even after reducing I/O, the execution model matters. Postgres uses a **tuple-at-a-time** volcano model: each operator (scan, filter, aggregate) processes one row, passes it up to the next operator, which processes it, and so on. Each row crosses multiple function call boundaries. At 100M rows, that's hundreds of millions of virtual function calls. Branch prediction thrashes. CPU caches are useless because each call processes a trivially small amount of data.

Column stores like DuckDB use **vectorized execution**: operators process batches of values (typically 1024-2048) from a single column at a time. A filter operation on `pickup_time` processes 1024 timestamps in a tight loop, no function call overhead between rows, the data is contiguous in memory so the CPU prefetcher works perfectly, and the compiler can auto-vectorize the loop to use SIMD instructions (processing 4-8 values per CPU cycle with AVX2).

The difference between tuple-at-a-time and vectorized execution is typically 5-10x on CPU-bound queries, independent of the I/O savings from columnar layout. Combined with the I/O reduction, you get the 50-100x gap students will see in the practical.

Walk through a concrete execution example. The query:

```sql
SELECT DATE_TRUNC('month', pickup_time) AS month, AVG(fare_amount)
FROM trips
WHERE pickup_time >= '2023-01-01' AND pickup_time < '2024-01-01'
GROUP BY month
ORDER BY month;
```

In DuckDB's vectorized pipeline:

1. **Scan** the `pickup_time` column, 1024 values at a time. Apply the filter as a selection vector (a bitmask of which rows pass). No rows are materialized, just the bitmask.
2. **Scan** the `fare_amount` column, but only for rows that passed the filter (using the selection vector). This is **late materialization**, you don't read columns you don't need until you know which rows survive the filter.
3. **Hash aggregate**: compute `DATE_TRUNC('month', pickup_time)` on the surviving batch, hash it, accumulate partial sums and counts into 12 hash table buckets (one per month).
4. Repeat for the next 1024 rows.
5. After all batches: divide sums by counts, sort 12 rows by month, return.

At no point did the engine read the `passenger_count`, `trip_distance`, or any other column. At no point did it process a single row in isolation, everything was batched.

### Module C, Zone maps and segment elimination

Columnar files are typically organized into **row groups** (DuckDB) or **pages** (Parquet). Each row group might contain 100k-1M rows. For each column in each row group, the engine stores lightweight metadata: the minimum value, maximum value, null count, and distinct count. These are **zone maps** (also called min/max indexes or data skipping metadata).

When a query has a filter like `WHERE pickup_time >= '2023-01-01'`, the engine checks each row group's zone map for `pickup_time`. If a row group's max value is `2022-06-15`, that entire row group is skipped, no data read at all.

If the data is sorted by `pickup_time` (or partially sorted, as is common with time-series data), zone maps become extremely effective. You might skip 80% of row groups without reading a single byte of actual data. This is **segment elimination**, and it's why column stores get faster when data has natural ordering.

**Key insight:** zone maps are essentially free, a few bytes of metadata per column per row group, checked before any data is read. Compare this to Postgres B-tree indexes, which must be maintained on every write. Column stores optimize reads at zero write cost because the metadata is computed once when the row group is written.

### Module D, Why OLAP engines are terrible at OLTP

Now flip it around. If columnar layout is so superior for analytics, why not use it for OLTP too?

Because inserting a single row in a column store means writing to N separate locations (one per column). Updating a row means finding and modifying N column segments. Each column segment is typically compressed, you can't just modify one value in a dictionary-encoded block without potentially rewriting the entire block.

Column stores handle writes via **batch append**: accumulate rows in an in-memory buffer (a row-oriented write-ahead structure), then periodically flush to compressed columnar segments. This is great for bulk inserts and CDC ingestion. It's terrible for "insert one row and immediately read it back in a point query," which is the fundamental OLTP pattern.

This is the punchline of the lesson: **OLTP and OLAP optimize for opposite access patterns.** Row stores optimize for random access to complete rows. Column stores optimize for sequential access to individual columns across many rows. No single layout serves both. This tension drives the entire architecture of modern data systems, and it's why CDC (Lesson 5) and Kafka (Lesson 6) exist as bridges between the two worlds.

---

## Hour 2, Practical: DuckDB vs. Postgres on 100M rows

### Setup (15 min)

**Dataset:** NYC Taxi & Limousine Commission trip data. Use the 2023 Yellow Taxi Parquet files, roughly 38M rows for the full year. If students want the full visceral impact, combine 2-3 years for ~100M rows. DuckDB can read Parquet directly, which is the point, no separate loading step.

Download instructions:

```bash
# Download 2023 Yellow Taxi data (Parquet format)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{01..12}.parquet
```

**Postgres setup:** Same Docker container from Lesson 1 (`--cpus=4 --memory=8g`, give it a bit more for this lesson). Students create a table and load the data. The loading itself is instructive, it'll take minutes to load 100M rows into Postgres, while DuckDB reads the Parquet files with zero loading time.

```sql
-- Postgres table
CREATE TABLE trips (
    vendor_id INT,
    pickup_datetime TIMESTAMPTZ,
    dropoff_datetime TIMESTAMPTZ,
    passenger_count INT,
    trip_distance NUMERIC(10,2),
    pickup_location_id INT,
    dropoff_location_id INT,
    rate_code_id INT,
    payment_type INT,
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2)
);

-- Load from Parquet using a helper or CSV export
-- (Students can use ogr2ogr, pandas, or DuckDB itself to convert to CSV and then COPY)
```

**DuckDB setup:** Install the python package (`pip install duckdb`). No server, no Docker, no configuration. DuckDB runs in-process. This contrast is deliberate, the simplicity of an embedded analytical engine vs. the operational overhead of a server.

```python
import duckdb

con = duckdb.connect()
# DuckDB reads Parquet directly - no loading step
con.sql("SELECT COUNT(*) FROM 'yellow_tripdata_2023-*.parquet'")
```

### Phase 1, Head-to-head queries (25 min)

Students run identical queries in both Postgres and DuckDB and record wall-clock time. These queries are chosen to highlight different aspects of the columnar advantage:

**Query 1, Full aggregation (I/O bound):**

```sql
SELECT COUNT(*), AVG(fare_amount), AVG(tip_amount), AVG(trip_distance)
FROM trips;
```

This touches 3 columns out of 17. Postgres reads everything; DuckDB reads only what it needs. Expected difference: 20-50x.

**Query 2, Filtered aggregation (zone maps):**

```sql
SELECT DATE_TRUNC('month', pickup_datetime) AS month,
       payment_type,
       COUNT(*) AS trips,
       AVG(fare_amount) AS avg_fare,
       SUM(tip_amount) AS total_tips
FROM trips
WHERE pickup_datetime >= '2023-06-01' AND pickup_datetime < '2023-09-01'
GROUP BY month, payment_type
ORDER BY month, payment_type;
```

If data is roughly time-ordered (it is in the original Parquet files), DuckDB's zone maps skip row groups outside the date range. Postgres does a sequential scan unless there's an index on `pickup_datetime`, and even with an index, the random I/O pattern for a range covering millions of rows makes the index scan slower than a sequential scan.

**Query 3, High-cardinality GROUP BY (hash table pressure):**

```sql
SELECT pickup_location_id, dropoff_location_id,
       COUNT(*) AS trips,
       AVG(fare_amount) AS avg_fare
FROM trips
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY trips DESC
LIMIT 20;
```

~260 pickup locations x ~260 dropoff locations = ~67k groups. Both engines handle this, but DuckDB's vectorized hash aggregation is dramatically faster because it processes batches through the hash table instead of one row at a time.

**Query 4, Window function (CPU bound):**

```sql
SELECT pickup_location_id,
       pickup_datetime,
       fare_amount,
       AVG(fare_amount) OVER (
           PARTITION BY pickup_location_id
           ORDER BY pickup_datetime
           ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
       ) AS rolling_avg
FROM trips
WHERE pickup_location_id = 132  -- JFK Airport
ORDER BY pickup_datetime;
```

Window functions are CPU-intensive, sorting, partitioning, and computing rolling aggregates. This shows the vectorized execution advantage on pure compute, not just I/O.

Students should record all results in a table. The gaps should be visceral, seconds in DuckDB vs. minutes in Postgres (or Postgres just falling over on 100M rows with insufficient memory for sort/hash operations).

### Phase 2, EXPLAIN ANALYZE deep dive (20 min)

Now comes the actual learning. Running queries and seeing "DuckDB is faster" is not the deliverable. Understanding *why* is the deliverable.

**In DuckDB:**

```sql
EXPLAIN ANALYZE
SELECT DATE_TRUNC('month', pickup_datetime) AS month,
       AVG(fare_amount)
FROM 'yellow_tripdata_2023-*.parquet'
WHERE pickup_datetime >= '2023-06-01' AND pickup_datetime < '2023-09-01'
GROUP BY month
ORDER BY month;
```

DuckDB's `EXPLAIN ANALYZE` shows the physical pipeline with actual timings and row counts per operator. Students should identify:

- **Parquet scan operator**: how many row groups were scanned vs. skipped (this is zone map/segment elimination in action)
- **Filter operator**: how many rows survived, if zone maps were effective, fewer rows enter the filter than total table rows
- **Hash group by operator**: batch size, number of groups, time spent
- **Order by operator**: trivial for 12 rows

Also use DuckDB's `PRAGMA enable_profiling='json'` for a machine-readable breakdown they can include in their report.

**In Postgres:**

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT DATE_TRUNC('month', pickup_datetime) AS month,
       AVG(fare_amount)
FROM trips
WHERE pickup_datetime >= '2023-06-01' AND pickup_datetime < '2023-09-01'
GROUP BY month
ORDER BY month;
```

Students should note:

- **Seq Scan** on the trips table, `Buffers: shared hit=X read=Y`. The total buffers tell you how many 8KB pages were read. Multiply by 8KB to get the actual I/O. Compare this to the data DuckDB read.
- **Rows Removed by Filter**, Postgres read all rows, then threw away the ones outside the date range. DuckDB skipped them before reading.
- **HashAggregate**, single-row execution time vs. DuckDB's vectorized aggregation.

**Key exercise:** for each query, students must annotate the query plan with *which specific optimization* (columnar layout, compression, zone maps, vectorized execution, late materialization) explains the performance difference. "DuckDB is faster because it's a column store" is not an acceptable answer. "DuckDB read 280MB vs Postgres's 18GB because it only scanned 2 of 17 columns, skipped 8 of 12 row groups via zone maps on `pickup_datetime`, and the remaining columns were dictionary-compressed with a 4x ratio", that's the bar.

---

## Hour 3, Where column stores break and where they shine

### Experiment A, Point queries: DuckDB's weakness (15 min)

Run a point lookup:

```sql
-- In both Postgres and DuckDB
SELECT * FROM trips WHERE pickup_datetime = '2023-07-15 14:30:00';
```

In Postgres with a B-tree index on `pickup_datetime`, this returns in microseconds, seek the index, read one heap page, done. In DuckDB, there's no B-tree. It must scan zone maps to find candidate row groups, then scan those row groups to find matching rows. For a point lookup, this is dramatically slower than an indexed row store.

**Lesson:** column stores are not a replacement for row stores. They serve different access patterns. If your workload is "fetch one user's profile by ID," a column store is the wrong tool.

### Experiment B, Compression analysis (15 min)

DuckDB exposes storage metadata. Students can examine actual compression:

```sql
-- Create a persistent DuckDB database to inspect storage
CREATE TABLE trips AS SELECT * FROM 'yellow_tripdata_2023-*.parquet';

-- Check storage info
CALL pragma_storage_info('trips');
```

This shows, per column, the segment type, compression algorithm, and size. Students should note:

- `payment_type` (4 distinct values): likely dictionary-encoded, tiny on disk
- `pickup_datetime`: likely delta-encoded or bitpacked
- `fare_amount`: likely constant or dictionary-encoded for common values, with a heavier encoding for the tail

Compare the total storage size of the DuckDB table vs. the Postgres table (`pg_total_relation_size('trips')`). The ratio illustrates compression from columnar layout alone.

### Experiment C, The impact of data ordering (20 min)

This experiment makes zone maps click.

```sql
-- In DuckDB, create two copies of the data
CREATE TABLE trips_by_time AS
    SELECT * FROM trips ORDER BY pickup_datetime;

CREATE TABLE trips_shuffled AS
    SELECT * FROM trips ORDER BY RANDOM();
```

Now run the date-filtered query from Phase 1 on both tables. On `trips_by_time`, zone maps are highly effective, the date range maps cleanly to contiguous row groups, so most are skipped. On `trips_shuffled`, every row group contains rows from every date, so no row groups can be skipped. The same query on the shuffled table will be significantly slower.

**Key insight:** columnar storage benefits enormously from **sort order alignment** between the data layout and common query filters. This is why real analytical systems care about how data is ordered when it's loaded. Partitioning by date in data warehouses isn't just organizational, it's a performance optimization that enables segment elimination.

### Experiment D, Postgres with columnar extensions (10 min)

Brief demonstration that the row/column distinction isn't about the database brand, it's about the storage engine. Show that Postgres *can* do columnar storage via extensions:

- **Citus Columnar** (formerly `cstore_fdw`): columnar storage engine for Postgres
- **pg_analytics** (from ParadeDB): brings DuckDB-style columnar analytics into Postgres

This doesn't need hands-on benchmarking, just awareness. The point is that "Postgres is slow at analytics" is imprecise. *Postgres's row-oriented heap storage* is slow at analytics. The query execution model is separate from the storage model, and both matter.

### Final 20 minutes, Synthesis

Return to the motivating question: **why can the same hardware scan billions of rows in OLAP but chokes at 10k TPS in OLTP?**

Students should now answer this with specifics:

1. **I/O reduction**: columnar layout reads only needed columns (potentially 5-10x less data)
2. **Compression**: same-type contiguous values compress 3-10x better than mixed-type rows
3. **Segment elimination**: zone maps skip irrelevant data without reading it
4. **Vectorized execution**: batch processing enables CPU-friendly access patterns and SIMD
5. **Late materialization**: defer assembling complete rows until the last possible moment

And the inverse, why column stores can't do OLTP:

1. **Writes touch N column segments** per row inserted
2. **Point lookups lack efficient indexes** (no B-trees on columns)
3. **Compression blocks must be decompressed** to modify single values
4. **The batch-append write model** adds latency before data is queryable

This duality sets up the rest of the course: OLTP and OLAP need different engines, and the challenge is moving data between them efficiently. That's what Lessons 4-6 solve.

---

## Take-home deliverable

Annotated query plan analysis. For each of the 4 benchmark queries:

- The Postgres `EXPLAIN ANALYZE` output with annotations
- The DuckDB `EXPLAIN ANALYZE` output with annotations
- A written explanation (per query, not generic) identifying **which specific optimizations** account for the performance difference, with evidence from the plan output (bytes read, rows scanned vs. skipped, operator timings)
- The storage size comparison between Postgres and DuckDB, with an explanation of which compression schemes were applied to which columns

The report must not contain the phrase "DuckDB is faster because it's a column store." That's the conclusion, not the explanation.
