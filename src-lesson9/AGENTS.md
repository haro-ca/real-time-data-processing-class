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
- `src/trigger_sweep.py` — holds the rate fixed and sweeps Spark's trigger
  interval, plotting Spark vs. Flink p50 latency (the slide 5 plot). Runs on
  an isolated topic/checkpoint/data namespace so it can share the broker with
  other jobs (see Conventions).
- `src/trigger_sweep_extend.py` — adds new trigger points to an existing
  `trigger_sweep.json` by importing and reusing `trigger_sweep.run_round()`/
  `plot()` directly, so points already trusted aren't re-run. Used to push
  the sweep below 1s (found Spark's floor keeps falling to ~5s at a 250ms
  trigger — no plateau at 2s the way the original unmeasured claim assumed).
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
- Topics, the checkpoint dir, and the data dir are overridable via
  `L9_TOPIC_PREFIX`, `L9_CKPT_DIR`, and `L9_DATA_DIR` (see `config.py`, all
  default to the usual single-run locations). Set all three to run a benchmark
  on an isolated namespace so it can share the broker with another run without
  corrupting it — `trigger_sweep.py` does this with a `trig-` prefix. Two runs
  sharing the default topics/checkpoints/`data` **will** corrupt each other
  (zeroed windows, wrong report read); isolation is mandatory whenever another
  agent or job is live on the same broker.

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
- `--trigger` is a float everywhere (`spark_pipeline.py`, `benchmark.py`) so
  sub-second values like `0.5`/`0.25` work — Spark's own trigger string is
  built in milliseconds internally. `--drain-seconds`/`--warmup-seconds`/
  `--produce-duration` are still `int`-only; a caller that derives one of
  those from a fractional trigger (e.g. `20 + trigger`) must round it before
  passing it through, or the child `benchmark.py` process dies with an
  argparse error. This bit `trigger_sweep.py`'s `run_round()` once: the
  subprocess failure went unchecked, so it silently read the previous
  round's stale `latency_report.json` and returned it as if valid — three
  different trigger values ended up with byte-identical "measurements"
  before it was caught. `run_round()` now raises on a nonzero returncode
  instead of proceeding to read the report.
- Two Spark `local[*]` + Flink mini-cluster pairs running concurrently on the
  same machine (e.g. a manual test in one terminal while a sweep script runs
  in another) contend for CPU and *will* skew latency measurements even
  though topic isolation (`L9_TOPIC_PREFIX` etc.) prevents data corruption.
  Check `ps aux` for other `spark_pipeline.py`/`flink_pipeline.py` processes
  before starting an automated multi-round script.

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
