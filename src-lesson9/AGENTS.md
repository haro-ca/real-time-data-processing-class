# AI assistance context for src-lesson9

## What this code does

Lesson 9 benchmarks Spark Structured Streaming against PyFlink DataStream by
running the same 5-minute tumbling-window aggregation over a Kafka topic and
measuring how long each engine takes to emit each window result.

## Project layout

- `docker-compose.yml` — single-node Kafka (KRaft). Spark/PyFlink run on host.
- `docker-compose.flink-cluster.yml` — optional standalone Flink cluster.
- `pyproject.toml` — default `.venv` on Python 3.14 with `pyspark==4.0.1`,
  `confluent-kafka`, `numpy`, `matplotlib`.
- `src/setup_flink_venv.py` — creates `.venv-flink` on Python 3.11 and installs
  `apache-flink==1.19.1` (PyFlink does not support 3.14 and pins a conflicting
  `py4j` version).
- `src/config.py` — shared constants, SparkSession builder, helper that downloads
  the Flink Kafka connector JAR to `lib/` on first run.
- `src/setup_topics.py` — creates `orders`, `results-spark`, `results-flink`.
- `src/setup_flink_venv.py` — one-time setup of the Python 3.11 PyFlink venv.
- `src/producer.py` — controlled-rate order generator with `ts` (event time) and
  `produced_at_ms` (wall clock).
- `src/spark_pipeline.py` — Spark Structured Streaming, emits JSON results with
  `emit_ts_ms` to `results-spark`.
- `src/flink_pipeline.py` — PyFlink DataStream, emits JSON results with
  `emit_ts_ms` to `results-flink`.
- `src/analyze_latency.py` — drains both result topics, computes processing and
  end-to-end latency, writes `data/latency_cdf.png` and
  `data/latency_report.json`.
- `src/benchmark.py` — orchestrates the whole benchmark.

## Conventions

- Bootstrap is `localhost:19092` (Docker Kafka EXTERNAL listener).
- Spark uses `local[*]` and downloads the Kafka connector via
  `spark.jars.packages`.
- PyFlink runs in local mini-cluster mode and needs the Flink Kafka connector
  JAR; `config.ensure_flink_kafka_jar()` downloads it to `lib/` on first run.
- Both pipelines start from `earliest` on `orders` and write results to Kafka.
- Checkpoints live in `ckpt/spark` and `ckpt/flink`. Deleting them resets state.

## Hard constraints

- Default venv uses Python 3.14; PyFlink must run from `.venv-flink` on Python
  3.11 because `apache-flink` 1.19 does not support 3.14 and pins a different
  `py4j` version than Spark 4.
- Never share checkpoint directories between Spark and Flink.
- The `orders` topic must be created before producing (run `setup_topics.py`).
- Both pipelines must consume the **same** source data for an apples-to-apples
  comparison. The benchmark orchestrator produces first, then runs both engines.

## How to verify

1. `docker compose up -d`
2. `uv run python src/setup_topics.py`
3. `uv run python src/setup_flink_venv.py` (one-time)
4. `uv run python src/benchmark.py --rate 50 --produce-duration 60 --pipeline-runtime 120 --window-seconds 30`
4. Check `data/latency_report.json`: both engines should report non-empty latency
   percentiles, and Spark's p99 should be higher/more variable than Flink's.
