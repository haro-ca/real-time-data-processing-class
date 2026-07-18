# Lesson 9 — Micro-batch vs. true streaming: Spark vs. Flink latency benchmark

This lesson answers one question with data: **how much latency does Spark
Structured Streaming's micro-batch model add, and when does it matter?**

Students already built windowed aggregations in Spark (Lessons 7-8) using
5-minute tumbling windows. Here we re-implement the same aggregation logic in
PyFlink's DataStream API, instrument both engines, and compare latency CDFs.
The benchmark itself uses **much shorter windows** (default 15s, not 5
minutes) — the point isn't the window size, it's collecting enough closed
windows per engine to plot a real distribution instead of 1-2 points. The
recommendation at the end depends on the SLA, not on brand preference.

**Measurement methodology matters here.** Both pipelines are started *before*
the producer, and wait for confirmed readiness before any data flows. If the
producer ran first and finished before the engines started, both would
cold-start against a pre-existing backlog, and the measured "latency" would
be dominated by JVM/mini-cluster startup + backlog catch-up time — which is
roughly the same for both engines — instead of the actual micro-batch vs.
streaming difference the lesson is about.

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

4. Run the full benchmark (starts both pipelines, waits for readiness, then
   produces orders and generates CDFs):
   ```bash
   uv run python src/benchmark.py
   ```

   Defaults: `--rate 50 --produce-duration 240 --window-seconds 15
   --warmup-seconds 15 --drain-seconds 20`, i.e. ~16 windows per engine over
   about 5 minutes wall-clock. Increase `--produce-duration` (or lower
   `--window-seconds`) for a larger sample; the orchestrator warns if fewer
   than 10 windows are expected.

5. Inspect results:
   - `data/latency_cdf.png` — processing and end-to-end latency CDFs.
   - `data/latency_report.json` — p50/p95/p99 numbers.
   - `data/producer_summary.json` — events produced.

## Step-by-step (classroom mode)

If you want to talk through each engine separately instead of running the
orchestrator, **start both pipelines before the producer** — otherwise you're
measuring backlog catch-up time, not streaming latency (see methodology note
above).

1. Reset state if re-running — checkpoints *and* Kafka topics. Kafka retains
   messages across runs even after checkpoints are wiped, so skipping the
   topic reset silently mixes old results into the new analysis:
   ```bash
   rm -rf ckpt data/*.json data/*.png
   uv run python src/setup_topics.py --reset
   ```

2. In one terminal, start the Spark pipeline and wait for it to report ready
   (`Spark query running, subscribed to 'orders'.`):
   ```bash
   uv run python src/spark_pipeline.py --trigger 2 --window-seconds 15
   ```

3. In another terminal, start the Flink pipeline using its dedicated 3.11
   venv, and wait for `Flink job started: ...`:
   ```bash
   .venv-flink/bin/python src/flink_pipeline.py --window-seconds 15
   ```

4. Only once both report ready, produce orders (e.g., 4 minutes at 50
   events/second — enough for ~16 windows at 15s each):
   ```bash
   uv run python src/producer.py --rate 50 --duration-seconds 240
   ```

5. Once the producer finishes, give the pipelines ~20s to close the final
   window, then stop both with Ctrl-C.

6. Analyze:
   ```bash
   uv run python src/analyze_latency.py
   ```

## What to expect

With a 2-second Spark trigger, a 5-second watermark in both engines, and a
15-second window, measured on a single laptop (`uv run python
src/benchmark.py`, 16/16 windows closed):

| Metric | Spark | PyFlink |
|---|---|---|
| min processing latency | 7.0 s | 6.6 s |
| p50 processing latency | 8.0 s | 7.6 s |
| p99 processing latency | 9.0 s | 8.6 s |
| jitter (p99-p50) | 1.0 s | 1.0 s |

The honest headline: **Flink's latency sits at a consistently lower floor
across the whole distribution** — every percentile is ~400-500ms below
Spark's, and its CDF curve sits left of Spark's almost everywhere, not just
offset by a constant (that would be a measurement artifact, not signal — see
below). At this specific trigger/window config the *jitter* (p99-p50) came
out similar for both, because a 2s trigger against a 15s window doesn't leave
much room for Spark's batch-boundary quantization to show up in absolute
terms. Widen the gap with `--trigger 10` (see the experiment below) to see it
more clearly: Spark's floor is `max(trigger_interval, batch_processing_time)`,
which grows with the trigger; Flink's floor is watermark delay, which
doesn't. Spark's advantage isn't latency — it's operational simplicity and
throughput-per-resource at scale (see `throughput_sweep.py`).

**A sanity check worth knowing about:** the first time this benchmark ran
(before this methodology was fixed), Spark and Flink came out nearly
identical — two straight lines offset by a constant ~2.5s, both in the
30-65s range. That was a measurement artifact, not a result: the producer ran
to completion *before* either engine started, so both cold-started against a
full backlog and the numbers measured JVM/mini-cluster startup + backlog
catch-up (roughly equal for both engines) instead of steady-state processing.
If you ever see a *constant* gap between the two CDFs top-to-bottom, or
latencies in the tens-of-seconds range for single-digit-second windows,
suspect the same thing — check that Kafka topics were reset (`setup_topics.py
--reset`) and that both pipelines were live before producing.

Try `--trigger 1`, `--trigger 5`, and `--trigger 10` to see how Spark's curve
shifts. Diminishing returns appear below ~2 seconds: the per-batch planning and
commit overhead starts to dominate.

## Throughput vs. latency

```bash
uv run python src/throughput_sweep.py
```

Runs the benchmark across several producer rates (default `[20, 75, 250,
750]` events/s, resetting Kafka topics and checkpoints between rounds) and
plots each engine's median processing latency against the throughput
actually achieved. Takes ~10-15 minutes.

**What we measured on this laptop:** from 19 to 586 events/s, Spark's latency
stayed *exactly* flat at 8.0s and Flink stayed inside its own band (6.2-7.9s,
no clear trend) — neither engine's latency responded to throughput in this
range. That's a real result, not a bug: it means batch processing time never
got close to exceeding the 2s trigger at these rates, so we never triggered
the `max(trigger_interval, batch_processing_time)` floor shifting. The
batching-amortizes-overhead trade-off is real in principle, but seeing it on
a single laptop needs either much higher sustained load or a much tighter
trigger than tested here — see the trigger-floor demo below, which forces
the shift directly instead of hoping throughput gets there organically.

## Micro-demos

Three small, focused scripts that each isolate one specific claim from the
slides and measure it directly, instead of inferring it from the full
Spark-vs-Flink comparison where several effects are entangled.

```bash
uv run python src/demo_trigger_floor.py       # ~1 min, no Kafka needed
uv run python src/demo_watermark_bound.py     # ~5 min, Flink only
uv run python src/demo_idle_source_stall.py   # ~3 min, Flink only
```

- **`demo_trigger_floor.py`** — proves `latency floor = max(trigger_interval,
  batch_processing_time)` directly from Spark's own `recentProgress`
  telemetry. Uses Spark's built-in `rate` source (no Kafka) with a
  controllable artificial per-batch delay so the effect is reproducible
  regardless of host speed. Measured: same 500ms trigger, light work floors
  at ~401ms (trigger-bound), heavy work floors at ~1,707ms (batch-bound).
- **`demo_watermark_bound.py`** — proves Flink's latency floor tracks the
  watermark bound, not a trigger. Flink-only, sweeps `--watermark-seconds`
  across `[1, 5, 10]`. Measured: 3.1s / 5.5s / 12.2s — monotonically
  increasing with the watermark, roughly in step with it.
- **`demo_idle_source_stall.py`** — the classic Flink gotcha: a
  bounded-out-of-orderness watermark only advances on new records, so a
  quiet source freezes it and pending windows never fire — silently, no
  error. Produces a burst then goes quiet; compares `with_idleness(1s)`
  against no idleness handling. Measured: 2/2 windows closed with idleness,
  1/2 without — the trailing window stalled for the entire 30s observation
  window.

## Optional: Flink cluster mode

If you prefer a standalone Flink cluster instead of local PyFlink:

```bash
docker compose -f docker-compose.yml -f docker-compose.flink-cluster.yml up -d
```

Submit the job from inside the JobManager container (the host-side script uses
local mode by default, so cluster mode requires a manual submit for now).

## Files

- `src/config.py` — shared constants, SparkSession builder, Flink JAR helper.
- `src/setup_topics.py` — creates `orders`, `results-spark`, `results-flink`;
  `--reset` deletes and recreates them (do this between measurement runs).
- `src/setup_flink_venv.py` — one-time setup of the Python 3.11 PyFlink venv.
- `src/producer.py` — controlled-rate order generator with embedded timestamps.
- `src/spark_pipeline.py` — Spark Structured Streaming with latency instrumentation.
- `src/flink_pipeline.py` — PyFlink DataStream equivalent. `--watermark-seconds`
  and `--disable-idleness` exist for the micro-demos below, not normal runs.
- `src/analyze_latency.py` — consumes result topics, computes CDFs, writes report.
- `src/benchmark.py` — starts both pipelines, waits for readiness, then
  produces + drains + analyzes.
- `src/throughput_sweep.py` — runs the benchmark across several producer
  rates and plots measured throughput vs. latency (`data/throughput_sweep.png`).
- `src/demo_trigger_floor.py` — proves the Spark trigger/batch-time floor
  claim directly (see Micro-demos below).
- `src/demo_watermark_bound.py` — proves the Flink watermark floor claim
  directly.
- `src/demo_idle_source_stall.py` — demonstrates the idle-source watermark
  stall gotcha.
