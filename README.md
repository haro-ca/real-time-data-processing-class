# Real-Time Data Processing

A 12-lesson course that starts with a single Postgres node and ends with a full streaming pipeline, OLTP → CDC → Kafka → stream processing → real-time OLAP → API. Each lesson is 3 hours: roughly 1 hour of theory and 2 hours of hands-on work.

## Prerequisites

- Python 3.13+
- Docker and Docker Compose
- Familiarity with SQL and basic Python async (`asyncio`)

## Tech stack

| Component | Tool | Introduced in |
|---|---|---|
| OLTP | PostgreSQL | Lesson 1 |
| Distributed OLTP | CockroachDB | Lesson 2 |
| Batch OLAP | DuckDB | Lesson 3 |
| Event streaming | Apache Kafka (KRaft) | Lesson 6 |
| Stream processing | PySpark Structured Streaming | Lesson 7 |
| True streaming | Apache Flink (PyFlink) | Lesson 9 |
| Real-time OLAP | ClickHouse | Lesson 10 |
| API layer | FastAPI | Lesson 10 |
| Schema registry | Confluent Schema Registry | Lesson 11 |
| Monitoring | Grafana | Lesson 11 |
| Orchestration (shown, not used) | Airflow, Dagster | Lesson 4 |

## AI assistance

AI coding assistants are strongly encouraged throughout the course. We recommend [opencode](https://github.com/nicepkg/opencode) (no credit card needed), but any agent works: Claude Code, GitHub Copilot, Windsurf Cascade, Codex, etc.

## Deliverables

Each lesson's deliverable is a GitHub repository containing:

- Working code
- `README.md` explaining the approach and findings
- `AGENTS.md` or `CLAUDE.md` documenting project context and conventions for AI assistants

Submit via pull request (preferred) or zip upload to the course drive.

---

## Syllabus

### Lesson 1, How much can a single OLTP node handle?

**Theory:** PostgreSQL internals, WAL, MVCC, buffer pool, vacuum. What actually bottlenecks first (CPU, I/O, locks, connections). Amdahl's law applied to database workloads.

**Practical:** Build a Python load generator (`asyncpg` + `asyncio`) and benchmark Postgres to failure. Identify which resource saturated and at what TPS. Not `pgbench`, students build their own, because understanding the instrumentation matters.

**Deliverable:** Bottleneck analysis with flame graphs (`py-spy`) and `pg_stat_statements` output.

---

### Lesson 2, What happens when you distribute OLTP?

**Theory:** CAP theorem (properly, not the hand-wavy version), consensus protocols (Raft), distributed transactions (2PC), the costs of coordination. Clock skew and hybrid logical clocks.

**Practical:** Deploy a 3-node CockroachDB cluster (Docker Compose). Run the same workload from Lesson 1. Measure the latency penalty of distributed transactions. Kill a node mid-workload and observe what happens.

**Deliverable:** Comparative analysis, single Postgres vs. distributed, with latency histograms (p50/p95/p99).

---

### Lesson 3, Why is OLAP a fundamentally different problem?

**Theory:** Row stores vs. column stores. Vectorized execution. Zone maps, late materialization, compression schemes (dictionary, RLE, delta). Why the same hardware can scan billions of rows in OLAP but chokes at 10k TPS in OLTP.

**Practical:** DuckDB vs. Postgres head-to-head on NYC taxi data (~100M rows). Write analytical queries and use `EXPLAIN ANALYZE` to trace execution in both engines. The gap should be visceral.

**Deliverable:** Annotated query plans explaining which specific optimizations account for DuckDB's 50-100x advantage.

---

### Lesson 4, Classical batch ETL and why "just move the data" is hard

**Theory:** ETL vs ELT. Idempotency. Slowly changing dimensions (SCD types). Schema evolution. Why orchestration exists (DAGs, dependency resolution, retry semantics). The "exactly-once in batch" problem.

**Practical:** Build a batch pipeline in raw Python, extract from Postgres, transform with DuckDB, load into an analytical target. Inject a failure mid-pipeline and make it recover without duplicates. Then see the same pipeline as a pre-written Airflow DAG and Dagster asset pipeline.

**Deliverable:** Pipeline code that is idempotent, run it 3 times, prove the result is identical.

---

### Lesson 5, CDC: the bridge between OLTP and everything else

**Theory:** Polling vs. log-based CDC. WAL decoding in Postgres (logical replication slots, `pgoutput` plugin). Debezium architecture. The outbox pattern.

**Practical:** Set up Postgres logical replication in Python (`psycopg3` logical replication API). Capture inserts/updates/deletes as a stream of events. Optional 10-minute expansion showing Debezium as the production-grade approach.

**Deliverable:** A CDC consumer that maintains a materialized view in DuckDB, kept in sync with the OLTP source.

---

### Lesson 6, Event streaming fundamentals (Kafka)

**Theory:** Log-structured storage. Partitions, offsets, consumer groups. Exactly-once semantics (idempotent producers, transactional consumers). Kafka's ISR mechanism. Why Kafka is a commit log, not a message queue.

**Practical:** Deploy Kafka (KRaft mode, no ZooKeeper). Write producers and consumers with `confluent-kafka-python`. Experiment with partition assignment, rebalancing, and consumer lag. Ends with a bridge to Lesson 7, mapping manual consumer work to how Spark abstracts it.

**Deliverable:** A consumer that handles rebalancing correctly and reports its own lag metrics.

---

### Lesson 7, Stream processing I: stateless transformations and windowing

**Theory:** Stream processing topology (sources, operators, sinks). Stateless transforms (map, filter, flatMap). Windowing: tumbling, sliding, session. Event time vs. processing time. Watermarks and the completeness/latency tradeoff.

**Practical:** PySpark Structured Streaming. Build a pipeline that consumes CDC events via Kafka and computes windowed aggregations (revenue per 5-minute tumbling window). Inject late events and observe watermark behavior.

**Deliverable:** Pipeline with configurable allowed lateness, demonstrating what happens when events arrive after the window closes.

---

### Lesson 8, Stream processing II: stateful operations and exactly-once

**Theory:** Stateful stream processing, joins (stream-stream, stream-table), sessionization, deduplication. State backends. Checkpointing and savepoints. Exactly-once end-to-end (source → processor → sink). The two generals problem.

**Practical:** Build a stateful join in PySpark, enrich transactions with customer data from a compacted Kafka topic. Write to Postgres with idempotent upserts. Kill the processor, restart, and verify no data loss or duplication. The hardest exercise in the course.

**Deliverable:** A fault-tolerant stateful pipeline with documented proof of exactly-once behavior across restarts.

---

### Lesson 9, Micro-batch vs. true streaming: Spark vs. Flink

**Theory:** Micro-batch execution model (Spark's trigger intervals, batch boundaries). Flink's record-at-a-time model. PyFlink DataStream API and Flink SQL. Throughput vs. latency curves. When micro-batch is good enough and when it isn't.

**Practical:** Re-implement the Lesson 7 pipeline in PyFlink (DataStream API). Measure p99 latency for both engines on identical workloads. Brief Flink SQL demonstration.

**Deliverable:** Benchmark report with latency CDFs for both engines and an architectural recommendation for three latency SLA scenarios.

---

### Lesson 10, Real-time OLAP: serving the results

**Theory:** Pre-aggregation vs. on-the-fly. Materialized views. Real-time OLAP engines (ClickHouse, Pinot, Druid) vs. batch OLAP (DuckDB, Snowflake). Ingestion latency vs. query latency. LSM trees in an OLAP context.

**Practical:** Deploy ClickHouse. Feed it from Kafka. Wire ClickHouse queries into a provided FastAPI skeleton to serve sub-second aggregations over live streaming data. Compare against DuckDB with batch-loaded data.

**Deliverable:** A live-updating analytical API endpoint with measured query latency under concurrent ingestion.

---

### Lesson 11, End-to-end pipeline: OLTP → CDC → Kafka → Spark → ClickHouse → API

**Theory:** Exactly-once across system boundaries. Schema registries and contract evolution (Avro). Backpressure propagation. Monitoring: consumer lag, processing latency, checkpoint duration. Observability as a first-class concern.

**Practical:** Wire together everything from Lessons 1-10 into a single pipeline (Docker Compose provided). Introduce a schema change in the source OLTP and propagate it through the entire pipeline without downtime. Integration hell, that's the point.

**Deliverable:** Running end-to-end pipeline with a Grafana dashboard showing lag, throughput, and latency at every stage.

---

### Lesson 12, Capstone: break everything, fix everything

**Theory:** Failure taxonomy (network partitions, slow nodes, poison pills, schema corruption, disk full). Chaos engineering principles. Graceful degradation patterns. Capacity planning for streaming systems.

**Practical:** CTF-style exercise. Students receive the instructor's reference pipeline with 6 injected failure scenarios spanning every layer of the stack. Diagnose and fix under time pressure.

**Deliverable:** Post-mortem document for each failure, root cause, detection method, fix, and prevention strategy, plus the code fixes.

---

## Course structure

Each lesson follows a consistent 3-hour format:

| Segment | Duration | Description |
|---|---|---|
| Theory | ~60 min | First-principles explanation, building mental models |
| Practical | ~90 min | Hands-on implementation, benchmarking, breaking things |
| Experiments & synthesis | ~30 min | Push further, discuss tradeoffs, connect to next lesson |

Detailed lesson plans are in [`lesson-details/en/`](lesson-details/en/).
