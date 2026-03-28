# Real-Time Data Processing

## Lesson 1 — How much can a single OLTP node handle?

**Theory:** PostgreSQL internals — WAL, MVCC, buffer pool, vacuum. What actually bottlenecks first (CPU, I/O, locks, connections). Amdahl's law applied to database workloads.

**Practical:** Students write a Python load generator (`asyncpg` + `asyncio`) and benchmark Postgres to failure. They must identify which resource saturated and at what TPS. Not `pgbench` — they build their own, because understanding the instrumentation matters.

**Deliverable:** A written analysis with flame graphs (`py-spy`) and `pg_stat_statements` output explaining their bottleneck.

---

## Lesson 2 — What happens when you distribute OLTP?

**Theory:** CAP theorem (properly, not the hand-wavy version), consensus protocols (Raft), distributed transactions (2PC), the costs of coordination. Clock skew and hybrid logical clocks.

**Practical:** Deploy CockroachDB or YugabyteDB (3 nodes, Docker Compose). Run the same workload from Lesson 1. Measure the latency penalty of distributed transactions. Then kill a node mid-workload and observe what happens.

**Deliverable:** Comparative analysis — single Postgres vs. distributed, with latency histograms (p50/p95/p99), not just averages.

---

## Lesson 3 — Why is OLAP a fundamentally different problem?

**Theory:** Row stores vs. column stores. Vectorized execution. Zone maps, late materialization, compression schemes (dictionary, RLE, delta). Why the same hardware can scan billions of rows in OLAP but chokes at 10k TPS in OLTP.

**Practical:** DuckDB deep dive. Students load a non-trivial dataset (NYC taxi, ~100M rows), write analytical queries, and use `EXPLAIN ANALYZE` to trace the execution pipeline. Compare the same queries against Postgres. The gap should be visceral.

**Deliverable:** Query plan annotations explaining why DuckDB is 50–100x faster on specific patterns.

---

## Lesson 4 — Classical batch ETL and why "just move the data" is hard

**Theory:** ETL vs ELT. Idempotency. Slowly changing dimensions (SCD types). Schema evolution. Why orchestration exists (DAGs, dependency resolution, retry semantics). The "exactly-once in batch" problem.

**Practical:** Build a batch pipeline in Python (no Airflow yet — raw code). Extract from the OLTP Postgres, transform with DuckDB, load into an analytical Postgres/DuckDB. Then introduce a failure mid-pipeline and make the pipeline recover correctly without duplicates.

**Deliverable:** Pipeline code that is provably idempotent — run it 3 times, get the same result.

---

## Lesson 5 — CDC: the bridge between OLTP and everything else

**Theory:** Polling vs. log-based CDC. WAL decoding in Postgres (logical replication slots, `pgoutput` plugin). Debezium architecture. The outbox pattern. Why CDC changes the game — you stop asking "what changed?" and start receiving "what changed."

**Practical:** Set up Postgres logical replication in Python (`psycopg3` logical replication API or a lightweight CDC consumer). Capture inserts/updates/deletes as a stream of events. Lean into the Rust+Python bindings angle if desired, or keep it pure Python.

**Deliverable:** A working CDC consumer that maintains a materialized view in DuckDB that stays in sync with the OLTP source.

---

## Lesson 6 — Event streaming fundamentals (Kafka)

**Theory:** Log-structured storage. Partitions, offsets, consumer groups. Exactly-once semantics (idempotent producers, transactional consumers). Kafka's ISR mechanism. Why Kafka is a commit log, not a message queue.

**Practical:** Deploy Kafka (KRaft mode, no ZooKeeper). Students write Python producers/consumers (`confluent-kafka-python`). Experiment with partition assignment, rebalancing, consumer lag. Deliberately produce out-of-order events and observe the consequences.

**Deliverable:** A consumer that handles rebalancing correctly and reports its own lag metrics.

---

## Lesson 7 — Stream processing I: stateless transformations and windowing

**Theory:** Stream processing topology (sources, operators, sinks). Stateless transforms (map, filter, flatMap). Windowing: tumbling, sliding, session. Event time vs. processing time. Watermarks and the completeness/latency tradeoff.

**Practical:** Faust (Python-native Kafka Streams) or PyFlink. Build a pipeline that consumes the CDC events from Lesson 5 via Kafka (Lesson 6) and computes windowed aggregations — e.g., revenue per 5-minute tumbling window. Inject late events and observe watermark behavior.

**Deliverable:** Pipeline that correctly handles late data with a configurable allowed lateness, and demonstrates what happens when events arrive after the window closes.

---

## Lesson 8 — Stream processing II: stateful operations and exactly-once

**Theory:** Stateful stream processing — joins (stream-stream, stream-table), sessionization, deduplication. State backends (RocksDB in Flink). Checkpointing and savepoints. Exactly-once end-to-end (source → processor → sink). The two generals problem in practical terms.

**Practical:** Build a stateful join — enrich a stream of transactions with customer data from a compacted Kafka topic. Checkpoint the state. Kill the processor, restart, and verify no data loss or duplication. This is the hardest exercise in the course.

**Deliverable:** A fault-tolerant stateful pipeline with documented proof of exactly-once behavior across restarts.

---

## Lesson 9 — Micro-batch vs. true streaming: Spark Structured Streaming vs. Flink

**Theory:** Micro-batch execution model (Spark's trigger intervals, batch boundaries). Continuous processing trade-offs. Throughput vs. latency curves. When micro-batch is good enough and when it isn't.

**Practical:** Implement the same pipeline from Lesson 7 in Spark Structured Streaming. Compare latency profiles (p99) against the Flink/Faust version. Measure what happens as you decrease the trigger interval toward "continuous."

**Deliverable:** Benchmark report with latency CDFs for both engines on identical workloads, with an architectural recommendation for three different latency SLA scenarios.

---

## Lesson 10 — Real-time OLAP: serving the results

**Theory:** Pre-aggregation vs. on-the-fly. Materialized views. Real-time OLAP engines (ClickHouse, Apache Pinot, Apache Druid) — how they differ from batch OLAP (DuckDB/Snowflake). Ingestion latency, query latency, and the tradeoff between them. LSM trees in an OLAP context.

**Practical:** Deploy ClickHouse. Feed it from the Kafka stream. Build a dashboard query layer (Python + ClickHouse HTTP interface) that serves sub-second aggregations over the last N minutes of streaming data. Compare against querying DuckDB with batch-loaded data.

**Deliverable:** A live-updating analytical endpoint with measured query latency under concurrent ingestion.

---

## Lesson 11 — End-to-end pipeline: OLTP → CDC → Kafka → Stream Processing → Real-time OLAP → API

**Theory:** Exactly-once across system boundaries. Schema registries and contract evolution (Avro/Protobuf). Backpressure propagation. Monitoring: consumer lag, processing latency, checkpoint duration. Observability as a first-class concern.

**Practical:** Wire together everything from lessons 1–10 into a single end-to-end pipeline. Introduce a schema change in the source OLTP and propagate it through the entire pipeline without downtime. This is integration hell — that's the point.

**Deliverable:** Running end-to-end pipeline with a Grafana dashboard showing lag, throughput, and latency at every stage.

---

## Lesson 12 — Capstone: break everything, fix everything

**Theory:** Failure taxonomy (network partitions, slow nodes, poison pills, schema corruption, clock skew, disk full). Chaos engineering principles. Graceful degradation patterns. Capacity planning for streaming systems.

**Practical:** Students receive a pre-built pipeline (the instructor's reference implementation) with 5 injected failure scenarios they must diagnose and fix under time pressure. Think CTF-style. Examples: a poison pill event that crashes the stream processor, a partition hotspot causing consumer lag spiral, a schema incompatibility that silently drops fields.

**Deliverable:** Post-mortem document for each failure — root cause, detection method, fix, and prevention strategy.
