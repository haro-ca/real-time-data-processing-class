"""Throughput-vs-latency sweep for Lesson 9.

Runs the full benchmark once per rate in RATES (see benchmark.py for the
pipelines-first / warmup / drain methodology), resetting Kafka topics and
checkpoints between rounds, and plots each engine's median processing
latency against the throughput actually achieved. This replaces the
hand-drawn, illustrative throughput-vs-latency diagram on slide 5 with real
measurements from this machine.

Median (p50), not p99, is the plotted metric: each round only accumulates a
handful of windows, and p99 of a small sample is close to meaningless (it's
basically just the max). p99 is still recorded per round in the JSON output
for reference.

Both engines here run at a fixed degree of parallelism (Spark: local[*],
Flink: parallelism=1 — see flink_pipeline.py). At high throughput this is
not an apples-to-apples resource comparison, and that's worth saying out
loud rather than hiding: if Flink's latency degrades sharply at the top
rate, that's a parallelism-1 ceiling, not a verdict on streaming engines in
general.

Takes roughly len(RATES) * (WARMUP_SECONDS + PRODUCE_DURATION +
DRAIN_SECONDS + ~15s analysis/reset overhead).

Usage:
    uv run python src/throughput_sweep.py
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt

from config import DATA_DIR, ROOT, banner

RATES = [20, 75, 250, 750]
WINDOW_SECONDS = 10
PRODUCE_DURATION = 75
WARMUP_SECONDS = 10
DRAIN_SECONDS = 20


def reset_state() -> None:
    subprocess.run([sys.executable, "src/setup_topics.py", "--reset"], cwd=ROOT, check=True)
    ckpt = ROOT / "ckpt"
    if ckpt.exists():
        shutil.rmtree(ckpt)


def run_round(rate: int) -> dict:
    reset_state()
    cmd = [
        sys.executable, "src/benchmark.py",
        "--rate", str(rate),
        "--produce-duration", str(PRODUCE_DURATION),
        "--window-seconds", str(WINDOW_SECONDS),
        "--warmup-seconds", str(WARMUP_SECONDS),
        "--drain-seconds", str(DRAIN_SECONDS),
    ]
    print(f"\n>>> rate={rate}/s: {' '.join(cmd)}", file=sys.stderr)
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"WARNING: benchmark run failed for rate={rate} (exit {result.returncode})", file=sys.stderr)

    report = json.loads((DATA_DIR / "latency_report.json").read_text())
    summary = json.loads((DATA_DIR / "producer_summary.json").read_text())

    # Keep a per-round paper trail so a bad point can be traced back to its
    # source report instead of just trusting the aggregated summary below.
    (DATA_DIR / f"sweep_rate_{rate}.json").write_text(
        json.dumps({"report": report, "producer_summary": summary}, indent=2) + "\n"
    )

    return {
        "requested_rate": rate,
        "effective_rate": summary["effective_rate"],
        "spark_windows": report["spark"]["windows"],
        "spark_p50_ms": report["spark"]["processing_latency_ms"]["p50"],
        "spark_p99_ms": report["spark"]["processing_latency_ms"]["p99"],
        "flink_windows": report["flink"]["windows"],
        "flink_p50_ms": report["flink"]["processing_latency_ms"]["p50"],
        "flink_p99_ms": report["flink"]["processing_latency_ms"]["p99"],
    }


def plot(results: list[dict], path: Path) -> None:
    rates = [r["effective_rate"] for r in results]
    spark_p50 = [r["spark_p50_ms"] for r in results]
    flink_p50 = [r["flink_p50_ms"] for r in results]

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(rates, spark_p50, marker="o", linewidth=2, label="Spark (p50 processing latency)")
    ax.plot(rates, flink_p50, marker="o", linewidth=2, label="Flink (p50 processing latency)")
    ax.set_xlabel("Throughput (events/s, actually achieved)")
    ax.set_ylabel("Median processing latency (ms, log scale)")
    ax.set_yscale("log")
    ax.set_title("Throughput vs. latency — measured on this machine")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"chart saved to {path}")


def main():
    banner(
        "Lesson 9 throughput sweep",
        f"rates:            {RATES}",
        f"window:           {WINDOW_SECONDS}s",
        f"produce duration: {PRODUCE_DURATION}s per rate",
    )

    results = [run_round(rate) for rate in RATES]

    out_path = DATA_DIR / "throughput_sweep.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nsweep results saved to {out_path}")
    print(json.dumps(results, indent=2))

    plot(results, DATA_DIR / "throughput_sweep.png")


if __name__ == "__main__":
    main()
