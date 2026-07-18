"""Demo: Spark's micro-batch latency floor is max(trigger_interval, batch_processing_time).

Self-contained — uses Spark's built-in `rate` source, no Kafka needed. Runs
two scenarios back to back at the SAME 500ms trigger, with a controllable
artificial delay standing in for "expensive batch work" so the effect is
reproducible regardless of how fast the host machine is:

  1. light: ~50ms of work per batch (well under the trigger)   -> floor = trigger interval
  2. heavy: ~1500ms of work per batch (well over the trigger)  -> floor = batch processing time

Batch durations come straight from Spark's own StreamingQuery.recentProgress
telemetry (durationMs.triggerExecution) — this is Spark self-reporting how
long each micro-batch actually took, not something inferred from window
timestamps like the main benchmark.

Usage:
    uv run python src/demo_trigger_floor.py
"""

import json
import shutil
import time

import matplotlib.pyplot as plt

from config import CKPT_DIR, DATA_DIR, banner, build_spark

TRIGGER_MS = 500
WARMUP_BATCHES = 3  # dropped from the reported set: query planning + JIT warm-up skews batch 0 badly
NUM_BATCHES = WARMUP_BATCHES + 12
SCENARIOS = [
    ("light (~50ms work/batch)", 0.05),
    ("heavy (~1500ms work/batch)", 1.5),
]


def run_scenario(spark, work_seconds: float) -> list[dict]:
    ckpt = CKPT_DIR / "trigger_floor_demo"
    if ckpt.exists():
        shutil.rmtree(ckpt)

    def process_batch(batch_df, batch_id):
        batch_df.count()  # force materialization so the batch does real work
        if work_seconds > 0:
            time.sleep(work_seconds)

    stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    query = (
        stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=f"{TRIGGER_MS} milliseconds")
        .option("checkpointLocation", str(ckpt))
        .start()
    )

    while query.isActive and len(query.recentProgress) < NUM_BATCHES:
        time.sleep(0.5)
    query.stop()

    progress = [json.loads(p.json) for p in query.recentProgress]
    records = [
        {
            "batch_id": p["batchId"],
            "timestamp": p["timestamp"],
            "trigger_execution_ms": p.get("durationMs", {}).get("triggerExecution", 0),
        }
        for p in progress
        if "durationMs" in p
    ]
    return records[WARMUP_BATCHES:]


def plot(results: dict[str, list[dict]], path) -> None:
    fig, axes = plt.subplots(1, len(results), figsize=(6.5 * len(results), 5), sharey=True)
    for ax, (label, records) in zip(axes, results.items()):
        durations = [r["trigger_execution_ms"] for r in records]
        ax.bar(range(len(durations)), durations, color="#5cd6e8")
        ax.axhline(TRIGGER_MS, color="#ff6e54", linestyle="--", linewidth=2, label=f"trigger = {TRIGGER_MS}ms")
        ax.set_title(label)
        ax.set_xlabel("batch #")
        ax.set_ylabel("actual batch duration (ms)")
        ax.legend()
        ax.grid(True, alpha=0.3)
    fig.suptitle("Latency floor = max(trigger_interval, batch_processing_time)")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"chart saved to {path}")


def main():
    banner(
        "Trigger floor demo",
        f"trigger:     {TRIGGER_MS}ms (same for both scenarios)",
        f"batches/run: {NUM_BATCHES}",
    )

    spark = build_spark("lesson9-trigger-floor-demo")
    results = {}
    for label, work_seconds in SCENARIOS:
        print(f"\n>>> scenario: {label}")
        records = run_scenario(spark, work_seconds)
        results[label] = records
        durations = [r["trigger_execution_ms"] for r in records]
        avg = sum(durations) / len(durations) if durations else 0
        print(f"    avg actual batch duration: {avg:.0f}ms  (requested trigger: {TRIGGER_MS}ms)")
    spark.stop()

    out_path = DATA_DIR / "trigger_floor_demo.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nresults saved to {out_path}")

    plot(results, DATA_DIR / "trigger_floor_demo.png")


if __name__ == "__main__":
    main()
