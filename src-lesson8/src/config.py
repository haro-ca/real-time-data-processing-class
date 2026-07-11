"""Shared wiring for the Lesson 8 PySpark Structured Streaming demos.

Environment overrides (all optional, sane localhost defaults):
    KAFKA_BOOTSTRAP   bootstrap servers (default: localhost:19092)
    POSTGRES_URL      default postgresql://lesson8:lesson8@localhost:5432/lesson8
    JAVA_HOME         must point at a Java 17 JDK — Spark 4 won't start without it
"""

import json
import os
import sys
import threading
from pathlib import Path

import pyspark

# ── Kafka ────────────────────────────────────────────────────────────────────
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
TRANSACTIONS_TOPIC = "transactions"
CUSTOMERS_TOPIC = "customers"

# ── Postgres ─────────────────────────────────────────────────────────────────
POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://lesson8:lesson8@localhost:5432/lesson8"
)
DB_TABLE = "enriched_transactions"

# ── Directories ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
CKPT_DIR = ROOT / "ckpt"
CKPT_DIR.mkdir(exist_ok=True)
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
PROGRESS_FILE = DATA_DIR / "progress.jsonl"
PRODUCED_FILE = DATA_DIR / "produced.json"

# ── Maven packages for Spark runtime download ────────────────────────────────
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
PG_JDBC_JAR = "org.postgresql:postgresql:42.7.1"

# ── Teaching narration ───────────────────────────────────────────────────────
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


# ── Spark ────────────────────────────────────────────────────────────────────
def build_spark(app_name: str, shuffle_partitions: int = 4):
    """A local[*] SparkSession wired for Kafka and Postgres."""
    if not os.environ.get("JAVA_HOME"):
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

    packages = f"{KAFKA_PKG},{PG_JDBC_JAR}"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Schemas ──────────────────────────────────────────────────────────────────
def transaction_schema():
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    return StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_time", StringType()),
    ])


def customer_schema():
    from pyspark.sql.types import StringType, StructField, StructType
    return StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("tier", StringType()),
        StructField("region", StringType()),
    ])


# ── Source reads ─────────────────────────────────────────────────────────────
def read_transactions(spark, starting: str = "earliest", max_per_trigger: int | None = None):
    from pyspark.sql.functions import col, from_json, to_timestamp

    reader = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TRANSACTIONS_TOPIC)
              .option("startingOffsets", starting))
    if max_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
    raw = reader.load()
    parsed = (raw.select(from_json(col("value").cast("string"), transaction_schema()).alias("d"))
              .select("d.*"))
    return parsed.withColumn("transaction_time", to_timestamp("transaction_time"))


def read_customers_static(spark):
    """Batch read of the compacted `customers` topic, keeping only the latest
    record per customer_id. This is the recommended stream-static join path."""
    from pyspark.sql import Window
    from pyspark.sql.functions import col, from_json, row_number

    raw = (spark.read
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", CUSTOMERS_TOPIC)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    parsed = (raw.select(from_json(col("value").cast("string"), customer_schema()).alias("d"),
                         col("timestamp").alias("kafka_ts"),
                         col("offset").alias("kafka_offset"),
                         col("partition").alias("kafka_partition"))
              .select("d.*", "kafka_ts", "kafka_offset", "kafka_partition"))
    win = (Window.partitionBy("customer_id")
           .orderBy(col("kafka_ts").desc(),
                    col("kafka_partition").desc(),
                    col("kafka_offset").desc()))
    return (parsed.withColumn("rn", row_number().over(win))
            .filter(col("rn") == 1)
            .drop("rn", "kafka_ts", "kafka_offset", "kafka_partition"))


def read_customers_stream(spark, starting: str = "earliest", max_per_trigger: int | None = None):
    """Streaming read of customers, with a watermark. This is the advanced
    stream-stream join path and is not used by the main `streaming_join.py`."""
    from pyspark.sql.functions import col, from_json

    reader = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", CUSTOMERS_TOPIC)
              .option("startingOffsets", starting))
    if max_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
    raw = reader.load()
    return (raw.select(from_json(col("value").cast("string"), customer_schema()).alias("d"),
                       col("timestamp").alias("customer_update_time"))
            .select("d.*", "customer_update_time")
            .withWatermark("customer_update_time", "1 hour"))


# ── Progress logging ─────────────────────────────────────────────────────────
def summarize_progress(p: dict) -> dict:
    state = (p.get("stateOperators") or [{}])[0]
    return {
        "batchId": p.get("batchId"),
        "timestamp": p.get("timestamp"),
        "inputRowsPerSecond": round(p.get("inputRowsPerSecond") or 0, 1),
        "processedRowsPerSecond": round(p.get("processedRowsPerSecond") or 0, 1),
        "numInputRows": p.get("numInputRows", 0),
        "numRowsTotal": state.get("numRowsTotal", 0),
        "numRowsUpdated": state.get("numRowsUpdated", 0),
        "numRowsDroppedByWatermark": state.get("numRowsDroppedByWatermark", 0),
    }


class ProgressPump(threading.Thread):
    """Polls query.lastProgress and appends a JSON line per batch."""

    def __init__(self, query, echo: bool = True, interval: float = 1.0):
        super().__init__(daemon=True)
        self.query, self.echo, self.interval = query, echo, interval
        self._stop = threading.Event()
        self._seen = -1

    def run(self):
        PROGRESS_FILE.write_text("")
        while not self._stop.is_set():
            self._drain()
            self._stop.wait(self.interval)
        self._drain()

    def _drain(self):
        for p in self.query.recentProgress or []:
            bid = p.get("batchId", -1)
            if bid <= self._seen:
                continue
            self._seen = bid
            s = summarize_progress(p)
            with PROGRESS_FILE.open("a") as fh:
                fh.write(json.dumps(s) + "\n")
            if self.echo:
                print(f"  Batch {s['batchId']:>3} | "
                      f"Throughput: {s['processedRowsPerSecond']:>6} rows/s | "
                      f"State: {s['numRowsTotal']:>4} rows | "
                      f"Input: {s['numInputRows']:>4} rows")

    def stop(self):
        self._stop.set()
