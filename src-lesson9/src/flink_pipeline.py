"""Lesson 9 PyFlink DataStream benchmark.

Reads the `orders` topic, computes 5-minute tumbling-window aggregates, and
emits each result to `results-flink` with an emit timestamp. The latency analyzer
compares these emits against the Spark Structured Streaming run.

Usage:
    uv run python src/flink_pipeline.py --max-time 900
"""

import argparse
import json
import sys
import time
from datetime import datetime

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import Time, TumblingEventTimeWindows

from config import (
    BOOTSTRAP,
    FLINK_CKPT,
    FLINK_RESULTS_TOPIC,
    ORDERS_TOPIC,
    WINDOW_SECONDS,
    banner,
    ensure_flink_kafka_jar,
)


class ParseOrder(MapFunction):
    def map(self, value: str) -> dict:
        return json.loads(value)


class OrderTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        return int(datetime.fromisoformat(value["ts"]).timestamp() * 1000)


class WindowAggregation(ProcessWindowFunction):
    def process(self, key: str, context, elements) -> str:
        orders = list(elements)
        count = len(orders)
        total = sum(o["amount"] for o in orders)
        avg = total / count if count else 0.0
        max_produced = max(o["produced_at_ms"] for o in orders) if orders else 0
        window = context.window()
        result = {
            "window_start_ms": window.start,
            "window_end_ms": window.end,
            "order_count": count,
            "total_revenue": round(total, 2),
            "avg_order_value": round(avg, 2),
            "max_produced_at_ms": max_produced,
            "emit_ts_ms": int(time.time() * 1000),
        }
        yield json.dumps(result)


def main():
    parser = argparse.ArgumentParser(description="PyFlink latency benchmark for L9")
    parser.add_argument("--max-time", type=int, default=0, help="cancel job after N seconds (0 = run until Ctrl-C)")
    parser.add_argument("--checkpointing", type=int, default=10_000, help="checkpoint interval in ms")
    parser.add_argument("--window-seconds", type=int, default=WINDOW_SECONDS, help="tumbling window size")
    args = parser.parse_args()

    banner(
        "PyFlink DataStream benchmark",
        f"checkpointing: {args.checkpointing}ms",
        f"watermark:     5s bounded out-of-orderness",
        f"window:        {args.window_seconds}s",
        f"output topic:  {FLINK_RESULTS_TOPIC}",
    )

    jar_path = ensure_flink_kafka_jar()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)
    env.enable_checkpointing(args.checkpointing)
    env.get_checkpoint_config().set_checkpoint_storage_dir(f"file://{FLINK_CKPT.resolve()}")
    env.add_jars(f"file://{jar_path.resolve()}")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(ORDERS_TOPIC)
        .set_group_id("flink-latency-benchmark")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "orders")

    parsed = ds.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())

    timestamped = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_seconds(1))
        .with_timestamp_assigner(OrderTimestampAssigner())
    )

    windowed = (
        timestamped
        .key_by(lambda _: "all", key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(args.window_seconds)))
        .process(WindowAggregation(), output_type=Types.STRING())
    )

    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(FLINK_RESULTS_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(serializer)
        .build()
    )

    windowed.sink_to(sink)

    job_client = env.execute_async("lesson9-flink-latency")
    print(f"Flink job started: {job_client.get_job_id()}")

    if args.max_time > 0:
        time.sleep(args.max_time)
        job_client.cancel()
        print("Flink job cancelled.")
    else:
        try:
            job_client.get_job_execution_result().result()
        except KeyboardInterrupt:
            job_client.cancel()
            print("\nStopping Flink job...", file=sys.stderr)


if __name__ == "__main__":
    main()
