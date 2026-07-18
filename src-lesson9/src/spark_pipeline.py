"""Lesson 9 Spark Structured Streaming benchmark.

Reads the `orders` topic, computes 5-minute tumbling-window aggregates, and
emits each result to `results-spark` with an emit timestamp. The latency
analyzer compares these emits against the same computation done in PyFlink.

Usage:
    uv run python src/spark_pipeline.py --trigger 2
"""

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    from_json,
    max as spark_max,
    struct,
    sum as spark_sum,
    to_json,
    unix_timestamp,
    window,
)

from config import (
    BOOTSTRAP,
    CKPT_DIR,
    DATA_DIR,
    ORDERS_TOPIC,
    SPARK_RESULTS_TOPIC,
    WINDOW_SECONDS,
    banner,
    build_spark,
    order_schema,
)

READY_MARKER = DATA_DIR / "spark.ready"


def main():
    parser = argparse.ArgumentParser(description="Spark latency benchmark for L9")
    parser.add_argument("--trigger", type=float, default=2, help="micro-batch trigger interval in seconds (fractional allowed, e.g. 0.5)")
    parser.add_argument("--max-time", type=int, default=0, help="stop after N seconds (0 = run until Ctrl-C)")
    parser.add_argument("--window-seconds", type=int, default=WINDOW_SECONDS, help="tumbling window size")
    args = parser.parse_args()
    trigger_ms = round(args.trigger * 1000)

    banner(
        "Spark Structured Streaming benchmark",
        f"trigger interval: {args.trigger}s ({trigger_ms}ms)",
        "watermark:        5s",
        f"window:           {args.window_seconds}s",
        f"output topic:     {SPARK_RESULTS_TOPIC}",
    )

    spark = build_spark("lesson9-spark-latency")
    schema = order_schema()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", ORDERS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.select(col("value").cast("string"))
        .select(from_json(col("value"), schema).alias("d"))
        .select("d.*")
    )

    window_spec = f"{args.window_seconds} seconds"
    agg = (
        parsed.withWatermark("ts", "5 seconds")
        .groupBy(window("ts", window_spec))
        .agg(
            count("*").alias("order_count"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            spark_max("produced_at_ms").alias("max_produced_at_ms"),
        )
        .withColumn("emit_ts_ms", (unix_timestamp(current_timestamp()) * 1000).cast("long"))
        .withColumn("window_start_ms", (unix_timestamp(col("window.start")) * 1000).cast("long"))
        .withColumn("window_end_ms", (unix_timestamp(col("window.end")) * 1000).cast("long"))
    )

    output = agg.select(
        to_json(
            struct(
                col("window_start_ms"),
                col("window_end_ms"),
                col("order_count"),
                col("total_revenue"),
                col("avg_order_value"),
                col("max_produced_at_ms"),
                col("emit_ts_ms"),
            )
        ).alias("value")
    )

    checkpoint = Path(CKPT_DIR) / "spark"
    query = (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", SPARK_RESULTS_TOPIC)
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=f"{trigger_ms} milliseconds")
        .start()
    )

    READY_MARKER.write_text(str(int(time.time() * 1000)))
    print(f"Spark query running, subscribed to '{ORDERS_TOPIC}'.", file=sys.stderr)

    if args.max_time > 0:
        time.sleep(args.max_time)
        query.stop()
    else:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping Spark query...", file=sys.stderr)
            query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
