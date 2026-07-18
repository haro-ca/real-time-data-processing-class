"""Collect results from both pipelines and produce latency CDFs + a report.

Assumes both pipelines have written windowed-aggregation JSON records to their
respective Kafka topics. Reads from the beginning, computes processing and
end-to-end latency for each engine, writes a PNG plot and a JSON report.

Usage:
    uv run python src/analyze_latency.py
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Consumer, KafkaError, TopicPartition

from config import BOOTSTRAP, DATA_DIR, FLINK_RESULTS_TOPIC, SPARK_RESULTS_TOPIC, banner


def drain_topic(topic: str, bootstrap: str, timeout: float = 5.0):
    """Read all messages from a topic from the beginning until no new messages."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"latency-analyzer-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    metadata = consumer.list_topics(topic=topic)
    partitions = metadata.topics[topic].partitions
    assignment = [TopicPartition(topic, p, 0) for p in partitions.keys()]
    consumer.assign(assignment)

    records = []
    empty_polls = 0
    max_empty_polls = 3

    while empty_polls < max_empty_polls:
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
            continue
        try:
            records.append(json.loads(msg.value().decode("utf-8")))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"warning: skipping malformed message: {e}", file=sys.stderr)

    consumer.close()
    return records


def compute_latencies(records: list[dict]):
    """Return processing and end-to-end latency arrays from result records."""
    processing = []
    end_to_end = []
    for r in records:
        emit = r.get("emit_ts_ms", 0)
        window_end = r.get("window_end_ms", 0)
        produced = r.get("max_produced_at_ms", 0)
        if emit and window_end:
            processing.append(emit - window_end)
        if emit and produced:
            end_to_end.append(emit - produced)
    return np.array(processing), np.array(end_to_end)


def percentile(arr: np.ndarray, p: float) -> float:
    return float(np.percentile(arr, p * 100)) if len(arr) else 0.0


def summarize(name: str, processing: np.ndarray, end_to_end: np.ndarray) -> dict:
    return {
        "engine": name,
        "windows": len(processing),
        "processing_latency_ms": {
            "min": round(float(np.min(processing)), 2) if len(processing) else None,
            "p50": percentile(processing, 0.50),
            "p95": percentile(processing, 0.95),
            "p99": percentile(processing, 0.99),
            "max": round(float(np.max(processing)), 2) if len(processing) else None,
        },
        "end_to_end_latency_ms": {
            "min": round(float(np.min(end_to_end)), 2) if len(end_to_end) else None,
            "p50": percentile(end_to_end, 0.50),
            "p95": percentile(end_to_end, 0.95),
            "p99": percentile(end_to_end, 0.99),
            "max": round(float(np.max(end_to_end)), 2) if len(end_to_end) else None,
        },
    }


def plot_cdf(spark_proc, flink_proc, spark_e2e, flink_e2e, path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for ax, title, s, f in [
        (axes[0], "Processing latency", spark_proc, flink_proc),
        (axes[1], "End-to-end latency", spark_e2e, flink_e2e),
    ]:
        for label, arr in [("Spark", s), ("Flink", f)]:
            if len(arr) == 0:
                continue
            sorted_arr = np.sort(arr)
            cdf = np.arange(1, len(sorted_arr) + 1) / len(sorted_arr)
            ax.plot(sorted_arr, cdf, label=label, linewidth=2)
        ax.axhline(0.99, color="gray", linestyle="--", alpha=0.5, label="p99")
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("CDF")
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"CDF plot saved to {path}")


def main():
    parser = argparse.ArgumentParser(description="analyze latency CDFs for L9")
    parser.add_argument("--timeout", type=float, default=5.0, help="consumer poll timeout in seconds")
    parser.add_argument("--output-dir", default=str(DATA_DIR), help="directory for report and plot")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    banner(
        "Latency analysis",
        f"reading topics: {SPARK_RESULTS_TOPIC}, {FLINK_RESULTS_TOPIC}",
        f"bootstrap:      {BOOTSTRAP}",
    )

    spark_records = drain_topic(SPARK_RESULTS_TOPIC, BOOTSTRAP, args.timeout)
    flink_records = drain_topic(FLINK_RESULTS_TOPIC, BOOTSTRAP, args.timeout)

    print(f"  Spark result records: {len(spark_records)}")
    print(f"  Flink result records: {len(flink_records)}")

    if not spark_records and not flink_records:
        print("No result records found. Run the pipelines first.", file=sys.stderr)
        sys.exit(1)

    spark_proc, spark_e2e = compute_latencies(spark_records)
    flink_proc, flink_e2e = compute_latencies(flink_records)

    report = {
        "spark": summarize("Spark Structured Streaming", spark_proc, spark_e2e),
        "flink": summarize("PyFlink DataStream", flink_proc, flink_e2e),
    }

    report_path = output_dir / "latency_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"\nLatency report saved to {report_path}")
    print(json.dumps(report, indent=2))

    plot_path = output_dir / "latency_cdf.png"
    plot_cdf(spark_proc, flink_proc, spark_e2e, flink_e2e, plot_path)


if __name__ == "__main__":
    main()
