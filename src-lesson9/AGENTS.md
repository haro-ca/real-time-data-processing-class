# AI assistance context for src-lesson9

## What this code does

Lesson 9 benchmarks Spark Structured Streaming against PyFlink DataStream by
running the same tumbling-window aggregation logic over a Kafka topic and
measuring how long each engine takes to emit each window result. The
benchmark uses short windows (default 15s, vs. the 5-minute windows from
Lessons 7-8) specifically to accumulate enough closed windows per engine
(dozens, not 1-2) for a real percentile/CDF comparison.

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
- `src/setup_topics.py` — creates `orders`, `results-spark`, `results-flink`;
  `--reset` deletes and recreates them (needed between runs — see Hard
  constraints).
- `src/setup_flink_venv.py` — one-time setup of the Python 3.11 PyFlink venv.
- `src/producer.py` — controlled-rate order generator with `ts` (event time) and
  `produced_at_ms` (wall clock).
- `src/spark_pipeline.py` — Spark Structured Streaming, emits JSON results with
  `emit_ts_ms` to `results-spark`. Writes `data/spark.ready` once the query is
  subscribed to Kafka.
- `src/flink_pipeline.py` — PyFlink DataStream, emits JSON results with
  `emit_ts_ms` to `results-flink`. Writes `data/flink.ready` once the job is
  submitted. Pins the Python UDF worker to `sys.executable` via
  `env.set_python_executable()` — without it, Flink resolves `python` from
  `$PATH` at job-submit time, which can silently select an interpreter
  without `pyflink` installed and fail every task with no visible error from
  the driver.
- `src/analyze_latency.py` — drains both result topics, computes processing and
  end-to-end latency, writes `data/latency_cdf.png` and
  `data/latency_report.json`.
- `src/benchmark.py` — orchestrates the whole benchmark: starts both
  pipelines, waits for `data/{spark,flink}.ready`, warms up, produces, drains,
  analyzes.
- `src/throughput_sweep.py` — runs the benchmark across several producer
  rates, plots measured throughput vs. p50 processing latency per engine.
- `src/demo_trigger_floor.py` — self-contained (Spark `rate` source, no
  Kafka), proves `max(trigger_interval, batch_processing_time)` from Spark's
  own `recentProgress` telemetry.
- `src/demo_watermark_bound.py` — Flink only, sweeps `--watermark-seconds`
  and shows latency tracks it directly.
- `src/demo_idle_source_stall.py` — Flink only, shows a quiet source stalls
  the watermark forever without `with_idleness()`.

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
  comparison — but "same data" does not mean "produce first." The benchmark
  orchestrator starts **both pipelines first**, waits for their `.ready`
  markers, warms up, then produces. Producing first (the original design)
  makes both engines cold-start against a full backlog, and the resulting
  "latency" measures JVM/mini-cluster startup + backlog catch-up — which is
  roughly equal for both engines — instead of the real micro-batch vs.
  streaming difference.
- Kafka retains topic data across runs even after checkpoints are wiped.
  Re-running without `setup_topics.py --reset` silently mixes prior runs'
  results into the current analysis (this happened during initial
  verification and produced nonsensical latency numbers before it was
  caught). Always reset topics before a measurement that matters.
- `setup_topics.py --reset` polls for confirmed topic deletion before
  recreating (`reset_topics()`). It used to sleep a fixed 2s instead, which
  was not reliably long enough — back-to-back rounds in a multi-round demo
  script would see `TOPIC_ALREADY_EXISTS` on recreation and silently keep
  reading the stale, growing old topic. If a multi-round script (any
  `demo_*.py`, `throughput_sweep.py`) ever reports suspiciously inflated or
  inconsistent window counts across rounds, this class of bug is the first
  thing to check — self-consistency across rounds (same window count every
  round, for a time-based window size) is a good tripwire.
- Window size must stay small relative to run length — enough windows need to
  close per engine (aim for 15-30+) for percentiles to mean anything. 5-minute
  windows over a 10-minute run only produce ~2 samples per engine, which is a
  connect-the-dots line, not a distribution.

## How to verify

1. `docker compose up -d`
2. `uv run python src/setup_topics.py --reset`
3. `uv run python src/setup_flink_venv.py` (one-time)
4. `uv run python src/benchmark.py --rate 50 --produce-duration 240 --window-seconds 15`
5. Check `data/latency_report.json`: both engines should report >=10 windows
   and non-empty latency percentiles, and Flink's latency should sit at a
   consistently lower floor than Spark's across the whole distribution (not
   just a lower p50) — a roughly constant gap top-to-bottom is a red flag for
   a measurement artifact (e.g. stale Kafka data or a cold-start bias), not a
   real architectural signal.
