"""Shared wiring for Lesson 9: Spark vs. PyFlink latency benchmark.

Environment overrides (all optional):
    KAFKA_BOOTSTRAP   bootstrap servers (default: localhost:19092)
    JAVA_HOME         Java 17 JDK for Spark; PyFlink needs Java 11/17
"""

import os
import sys
import urllib.request
from pathlib import Path

# Every entry point imports this module, so reconfigure here: without it,
# stdout redirected to a log file (as benchmark.py does for each subprocess)
# is block-buffered and shows nothing until the process exits, which makes
# tailing progress during a live run impossible.
sys.stdout.reconfigure(line_buffering=True)

# ── Kafka / topics ───────────────────────────────────────────────────────────
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
ORDERS_TOPIC = "orders"
SPARK_RESULTS_TOPIC = "results-spark"
FLINK_RESULTS_TOPIC = "results-flink"

# ── Directories ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
CKPT_DIR = ROOT / "ckpt"
CKPT_DIR.mkdir(exist_ok=True)
LIB_DIR = ROOT / "lib"
LIB_DIR.mkdir(exist_ok=True)

SPARK_CKPT = CKPT_DIR / "spark"
FLINK_CKPT = CKPT_DIR / "flink"

# ── Windowing defaults ───────────────────────────────────────────────────────
# 5 minutes matches the Lesson 7/8 slides, but can be shortened for a smoke test.
WINDOW_SECONDS = int(os.environ.get("L9_WINDOW_SECONDS", "300"))

# ── Flink connector JAR (downloaded on first run) ─────────────────────────────
FLINK_VERSION = "1.19.1"
KAFKA_CONNECTOR_VERSION = "3.2.0-1.19"  # connector release compatible with Flink 1.19.x
KAFKA_CONNECTOR_JAR = f"flink-sql-connector-kafka-{KAFKA_CONNECTOR_VERSION}.jar"
KAFKA_CONNECTOR_URL = (
    "https://repo1.maven.org/maven2/org/apache/flink/"
    f"flink-sql-connector-kafka/{KAFKA_CONNECTOR_VERSION}/{KAFKA_CONNECTOR_JAR}"
)
KAFKA_CONNECTOR_PATH = LIB_DIR / KAFKA_CONNECTOR_JAR


def ensure_flink_kafka_jar() -> Path:
    """Download the Kafka connector JAR that PyFlink needs for the source/sink."""
    if KAFKA_CONNECTOR_PATH.exists():
        return KAFKA_CONNECTOR_PATH
    print(f"Downloading Flink Kafka connector JAR...\n  {KAFKA_CONNECTOR_URL}", file=sys.stderr)
    urllib.request.urlretrieve(KAFKA_CONNECTOR_URL, KAFKA_CONNECTOR_PATH)
    return KAFKA_CONNECTOR_PATH


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


# ── SparkSession builder (same pattern as Lessons 7-8) ───────────────────────
def _kafka_pkg() -> str:
    """Lazily build the Spark Kafka connector Maven coordinate so the Flink
    venv can import this module without pyspark installed."""
    import pyspark
    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def build_spark(app_name: str, shuffle_partitions: int = 4):
    """A local[*] SparkSession wired for Kafka."""
    if not os.environ.get("JAVA_HOME"):
        for cand in (
            "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
        ):
            if os.path.isdir(cand):
                os.environ["JAVA_HOME"] = cand
                print(f"note: JAVA_HOME auto-detected → {cand}", file=sys.stderr)
                break
        else:
            print(
                "WARNING: JAVA_HOME is not set and no Homebrew openjdk@17 was found.\n"
                "  Spark 4 needs a Java 17 JDK:  brew install openjdk@17\n"
                "  then export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                file=sys.stderr,
            )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", _kafka_pkg())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Event schema (declared, never inferred) ──────────────────────────────────
def order_schema():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("order_id", LongType()),
            StructField("customer_id", IntegerType()),
            StructField("amount", DoubleType()),
            StructField("status", StringType()),
            StructField("ts", TimestampType()),        # event time
            StructField("produced_at_ms", LongType()), # wall-clock when sent to Kafka
        ]
    )
