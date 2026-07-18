# Lesson 9 — Micro-batch vs. true streaming: Spark vs. Flink latency benchmark

This lesson answers one question with data: **how much latency does Spark
Structured Streaming's micro-batch model add, and when does it matter?**

Students already built windowed aggregations in Spark (Lessons 7-8). Here we
re-implement the same 5-minute tumbling-window aggregation in PyFlink's
DataStream API, instrument both engines, and compare latency CDFs. The
recommendation at the end depends on the SLA, not on brand preference.

## Architecture

```
producer.py ──► Kafka topic: orders ◄── spark_pipeline.py ──► Kafka topic: results-spark
                              ◄── flink_pipeline.py ──► Kafka topic: results-flink
                                                              │
                                                              ▼
                                                    analyze_latency.py
                                                    CDF plots + JSON report
```

- **Kafka** runs in Docker (single-node KRaft).
- **Spark** runs on the host via `pyspark` (same pattern as Lessons 7-8) using
  the default `.venv` on **Python 3.14**.
- **PyFlink** runs in its own **Python 3.11** virtual environment
  (`.venv-flink`) because PyFlink 1.19 does not support Python 3.14 and pins a
  conflicting `py4j` version. A setup script creates this venv automatically.

## Prerequisites

- Java 17 JDK for Spark (`brew install openjdk@17`).
- Docker + Docker Compose for Kafka.
- Python 3.14 and Python 3.11 available to `uv`.
- `uv` for dependency management.

## Quick start

1. Start Kafka:
   ```bash
   docker compose up -d
   ```

2. Create the topics:
   ```bash
   uv run python src/setup_topics.py
   ```

3. Set up the PyFlink Python 3.11 venv (one-time):
   ```bash
   uv run python src/setup_flink_venv.py
   ```

4. Run the full benchmark (produces orders, runs both pipelines, generates CDFs):
   ```bash
   uv run python src/benchmark.py
   ```

   This takes roughly the sum of `--produce-duration` and
   `--pipeline-runtime` (defaults: 10 min + 15 min). For a shorter smoke test,
   shorten the window so results actually close during the run:
   ```bash
   uv run python src/benchmark.py --rate 50 --produce-duration 60 --pipeline-runtime 120 --window-seconds 30 --trigger 2
   ```

4. Inspect results:
   - `data/latency_cdf.png` — processing and end-to-end latency CDFs.
   - `data/latency_report.json` — p50/p95/p99 numbers.
   - `data/producer_summary.json` — events produced.

## Step-by-step (classroom mode)

If you want to talk through each engine separately instead of running the
orchestrator:

1. Reset checkpoints if re-running:
   ```bash
   rm -rf ckpt data/*.json data/*.png
   ```

2. Produce orders (e.g., 10 minutes at 100 events/second):
   ```bash
   uv run python src/producer.py --rate 100 --duration-seconds 600
   ```

3. In one terminal, run the Spark pipeline:
   ```bash
   uv run python src/spark_pipeline.py --trigger 2 --window-seconds 300
   # let it run until it catches up, then Ctrl-C
   ```

4. In another terminal, run the Flink pipeline using its dedicated 3.11 venv:
   ```bash
   .venv-flink/bin/python src/flink_pipeline.py --max-time 900 --window-seconds 300
   ```

5. After both finish, analyze:
   ```bash
   uv run python src/analyze_latency.py
   ```

## What to expect

With a 2-second Spark trigger and a 5-second watermark in both engines:

| Metric (guide values) | Spark | PyFlink |
|---|---|---|
| p50 processing latency | 3-5 s | 5-6 s |
| p99 processing latency | 6-10 s | 5.5-7 s |
| jitter (p99-p50) | 3-6 s | 0.5-1.5 s |

The headline finding: **Flink's latency distribution is tighter.** Spark's
variance comes from records that arrive just after a micro-batch starts and
wait for the next trigger boundary. Flink's latency is dominated by the
watermark delay, which is consistent.

Try `--trigger 1`, `--trigger 5`, and `--trigger 10` to see how Spark's curve
shifts. Diminishing returns appear below ~2 seconds: the per-batch planning and
commit overhead starts to dominate.

## Optional: Flink cluster mode

If you prefer a standalone Flink cluster instead of local PyFlink:

```bash
docker compose -f docker-compose.yml -f docker-compose.flink-cluster.yml up -d
```

Submit the job from inside the JobManager container (the host-side script uses
local mode by default, so cluster mode requires a manual submit for now).

## Files

- `src/config.py` — shared constants, SparkSession builder, Flink JAR helper.
- `src/setup_topics.py` — creates `orders`, `results-spark`, `results-flink`.
- `src/setup_flink_venv.py` — one-time setup of the Python 3.11 PyFlink venv.
- `src/producer.py` — controlled-rate order generator with embedded timestamps.
- `src/spark_pipeline.py` — Spark Structured Streaming with latency instrumentation.
- `src/flink_pipeline.py` — PyFlink DataStream equivalent.
- `src/analyze_latency.py` — consumes result topics, computes CDFs, writes report.
- `src/benchmark.py` — orchestrates producer + both pipelines + analyzer.
