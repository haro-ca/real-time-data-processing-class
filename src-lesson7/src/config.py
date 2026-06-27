"""Shared wiring for the Lesson 7 PySpark Structured Streaming demos.

Every script imports from here so the cluster address, the topic names, the
event schema, and the SparkSession construction live in exactly one place.

Environment overrides (all optional, sane localhost defaults):
    KAFKA_BOOTSTRAP   bootstrap servers (default: localhost:19092)
    L7_BASE           event-time anchor, "YYYY-MM-DDTHH:MM:SS" (default: today 12:00)
    JAVA_HOME         must point at a Java 17 JDK — Spark 4 won't start without it
"""

import json
import os
import sys
import threading
from datetime import datetime
from pathlib import Path

import pyspark

# ── Kafka ────────────────────────────────────────────────────────────────────
# From the host we dial the EXTERNAL advertised listener (localhost:19092). This
# is the same address L6's kafka-1 advertised, so the two are interchangeable.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")

TOPIC = "orders-cdc"                 # the source: order events, 5% of them late
REVENUE_TOPIC = "revenue-per-window" # stream_to_kafka.py sink #1
CUSTOMERS_TOPIC = "orders-per-customer"  # stream_to_kafka.py sink #2

# ── Where checkpoints live (offsets + window state, atomically) ──────────────
# This directory is the protagonist of Lesson 8. Deleting it = "start over from
# earliest, forget all window state".
ROOT = Path(__file__).parent.parent
CKPT_DIR = ROOT / "ckpt"
CKPT_DIR.mkdir(exist_ok=True)

DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
# The pipeline polls query.lastProgress and appends one JSON line per batch here.
# watch_progress.py (a SEPARATE terminal/process) tails it — because lastProgress
# is a handle in the driver's own process, a second process can't read it directly.
PROGRESS_FILE = DATA_DIR / "progress.jsonl"


def base_time() -> datetime:
    """The event-time anchor every demo hangs off: today at 12:00:00, local.

    Tumbling windows align to epoch boundaries in the session time zone, so a
    12:00 anchor gives clean [12:00,12:05) edges that match the slides. The
    injector's --at "12:05" resolves against this same day.
    """
    override = os.environ.get("L7_BASE")
    if override:
        return datetime.fromisoformat(override)
    now = datetime.now()
    return now.replace(hour=12, minute=0, second=0, microsecond=0)


def iso(dt: datetime) -> str:
    """Naive local ISO string. Producer and Spark run on the same box with the
    same session time zone, so naive timestamps stay consistent end-to-end."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def fmt_watermark(wm) -> str:
    """Render Spark's watermark in LOCAL time so it lines up with the window
    timestamps in the console.

    Spark reports eventTime.watermark as an ISO-8601 *UTC* string, but the console
    sink prints window start/end in the SESSION zone (local). Showing the watermark
    raw (UTC) next to local windows looks off by your UTC offset — a correct value
    that reads as a bug. The first micro-batches report the epoch (1970): that's
    Spark's 'no watermark yet', shown here as a dash rather than a 1970 timestamp.
    """
    if not wm:
        return "—"
    try:
        dt = datetime.fromisoformat(str(wm).replace("Z", "+00:00"))
    except ValueError:
        return str(wm)
    if dt.year < 2000:                     # epoch-0 sentinel: watermark not advanced yet
        return "— not set yet"
    return dt.astimezone().strftime("%H:%M:%S")


# ── Teaching narration (deliberately non-standard logs) ──────────────────────
# These demos talk to the student: banner() up front says what's about to happen
# and what to watch; lesson() at the end names the one idea the run just showed.
# Not how you'd log a production job — exactly how you want a teaching demo to read.
def banner(title: str, *lines: str) -> None:
    print("\n" + "═" * 78)
    print(f"  {title}")
    if lines:
        print("─" * 78)
        for ln in lines:
            print(f"  {ln}")
    print("═" * 78)


def lesson(*lines: str) -> None:
    print("\n" + "═" * 78)
    print("  ⟐  THE LESSON  ·  what this demo just showed")
    print("─" * 78)
    for ln in lines:
        print(f"  {ln}")
    print("═" * 78 + "\n")


# ── The Spark side ───────────────────────────────────────────────────────────
# The Kafka connector is a JVM library fetched from Maven on first run (slow
# once, cached after). Its version must match Spark's; Spark 4 is built for
# Scala 2.13, hence the _2.13 suffix.
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def build_spark(app_name: str, shuffle_partitions: int = 4):
    """A local[*] SparkSession wired for Kafka.

    shuffle_partitions is kept small (4) on purpose: state-store metrics are
    reported per shuffle partition, and 4 keeps numStateStoreInstances and the
    console output readable in class. Production would leave it at 200.
    """
    if not os.environ.get("JAVA_HOME"):
        # Spark 4 needs Java 17, and the error you'd otherwise get ("JAVA_HOME is not
        # set" / "Unable to locate a Java Runtime") eats 30 minutes of class time. So
        # auto-detect the standard Homebrew openjdk@17 (Apple Silicon, then Intel) and
        # set it in-process BEFORE the JVM launches — every `uv run python src/...`
        # command then just works with no manual export. Only warn if none is found.
        for cand in ("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                     "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"):
            if os.path.isdir(cand):
                os.environ["JAVA_HOME"] = cand
                print(f"note: JAVA_HOME auto-detected → {cand}", file=sys.stderr)
                break
        else:
            print("WARNING: JAVA_HOME is not set and no Homebrew openjdk@17 was found.\n"
                  "  Spark 4 needs a Java 17 JDK:  brew install openjdk@17\n"
                  "  then export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                  file=sys.stderr)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # tidy console: no progress bars stomping on the table output
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── The event schema (declared, never inferred — you can't infer over a stream) ─
# Imported lazily so plain Kafka producers (seed_events, inject_late) don't pull
# in Spark just to send JSON.
def order_schema():
    from pyspark.sql.types import (DoubleType, IntegerType, LongType,
                                    StringType, StructField, StructType,
                                    TimestampType)
    return StructType([
        StructField("order_id", LongType()),
        StructField("customer_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),   # ← the EVENT TIME column
    ])


def read_orders(spark, starting: str = "earliest", max_per_trigger: int | None = None):
    """The slide-18 ingestion, in one reusable place: readStream → from_json → flat.

    startingOffsets defaults to 'earliest' — Kafka's own default is 'latest',
    which shows you NOTHING on a pre-seeded topic and burns a confused half hour.
    max_per_trigger throttles rows/batch so the watermark advances visibly across
    several micro-batches instead of swallowing the whole topic in batch 0.
    """
    from pyspark.sql.functions import col, from_json

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", starting))
    if max_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
    raw = reader.load()
    # value is raw bytes (exactly like msg.value() in the L6 poll loop) — cast,
    # then parse with the declared schema.
    return (raw.select(from_json(col("value").cast("string"), order_schema()).alias("d"))
               .select("d.*"))


# ── Progress: the operational truth of a streaming query ─────────────────────
def summarize_progress(p: dict) -> dict:
    """Pull the numbers that matter out of a StreamingQueryProgress dict.

    The headline is numRowsDroppedByWatermark — every block of this course ends
    at one operational number (TPS, slot lag, consumer lag) and this is L7's.
    """
    state = (p.get("stateOperators") or [{}])[0]
    return {
        "batchId": p.get("batchId"),
        "timestamp": p.get("timestamp"),
        "inputRowsPerSecond": round(p.get("inputRowsPerSecond") or 0, 1),
        "processedRowsPerSecond": round(p.get("processedRowsPerSecond") or 0, 1),
        "numInputRows": p.get("numInputRows", 0),
        "numRowsTotal": state.get("numRowsTotal", 0),       # open windows remembered
        "numRowsUpdated": state.get("numRowsUpdated", 0),
        "numRowsDroppedByWatermark": state.get("numRowsDroppedByWatermark", 0),
        "watermark": p.get("eventTime", {}).get("watermark"),
    }


class ProgressPump(threading.Thread):
    """Polls query.lastProgress, echoes a one-liner, appends each batch to
    PROGRESS_FILE so a separate watch_progress.py can tail it. Daemon thread."""

    def __init__(self, query, echo: bool = True, interval: float = 1.0):
        super().__init__(daemon=True)
        self.query, self.echo, self.interval = query, echo, interval
        self._stop = threading.Event()
        self.dropped_total = 0
        self.peak_state = 0          # high-water mark of numRowsTotal
        self.last_state = 0
        self._seen = -1

    def run(self):
        PROGRESS_FILE.write_text("")          # fresh file per run
        while not self._stop.is_set():
            self._drain()
            self._stop.wait(self.interval)
        self._drain()                          # final sweep before exit

    def _drain(self):
        # Scan recentProgress (the last ~100 batches), not just lastProgress: a
        # tiny batch (e.g. 50 dropped events) can complete BETWEEN polls and a
        # single-snapshot poll would miss it — and miss its drops.
        for p in (self.query.recentProgress or []):
            bid = p.get("batchId", -1)
            if bid <= self._seen:
                continue
            self._seen = bid
            s = summarize_progress(p)
            self.dropped_total += s["numRowsDroppedByWatermark"]
            self.last_state = s["numRowsTotal"]
            self.peak_state = max(self.peak_state, s["numRowsTotal"])
            with PROGRESS_FILE.open("a") as fh:
                fh.write(json.dumps(s) + "\n")
            if self.echo:
                dropped = s['numRowsDroppedByWatermark']
                dropped_str = f"{dropped:>4}" if dropped == 0 else f"[{dropped:>3}]"
                # Watermark in LOCAL time so it lines up with the console's window
                # timestamps (Spark prints those in the session zone, not UTC).
                print(f"  Batch {s['batchId']:>3} | "
                      f"Throughput: {s['processedRowsPerSecond']:>6}/s | "
                      f"State: {s['numRowsTotal']:>4} rows | "
                      f"Dropped: {dropped_str} | "
                      f"Watermark: {fmt_watermark(s['watermark'])}")

    def stop(self):
        self._stop.set()
