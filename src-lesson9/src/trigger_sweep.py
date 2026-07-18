"""Trigger-interval sweep for Lesson 9 slide 5.

Holds the input rate fixed and sweeps Spark's micro-batch trigger interval,
plotting each engine's median processing latency against the trigger. This is
the honest, laptop-reproducible way to reveal the micro-batch vs. streaming
trade-off.

Why the trigger and not throughput? Spark's per-window latency floor is
`max(trigger_interval, batch_processing_time)`. On a single laptop the Python
producer can't generate enough load to push batch_processing_time past the
trigger (see throughput_sweep.py — the effective rate collapses well before
Spark saturates, so latency stays flat and the trade-off never shows). The
trigger, by contrast, is a knob we set directly. Sweeping it drives Spark's
floor up cleanly, while Flink has no trigger at all — its floor is the
watermark bound, which does not move with the trigger. The result is the
textbook divergence, and every point is a real measurement.

Both engines run the same fixed rate, window, and watermark; only Spark's
trigger changes between rounds. Kafka topics and checkpoints are reset between
rounds (see benchmark.py for the pipelines-first / warmup / drain methodology).

Median (p50), not p99, is the plotted metric: each round accumulates only a
handful of windows, and p99 of a small sample is basically the max. p99 is
still recorded per round in the JSON output for reference.

Takes roughly len(TRIGGERS) * (pipeline startup + WARMUP_SECONDS +
PRODUCE_DURATION + drain + ~15s analysis/reset overhead).

Usage:
    uv run python src/trigger_sweep.py
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt

from config import DATA_DIR, ROOT, banner

TRIGGERS = [1, 2, 5, 10]  # Spark micro-batch trigger interval in seconds
RATE = 50                 # fixed, well within what the producer sustains
WINDOW_SECONDS = 10
PRODUCE_DURATION = 120    # ~12 windows per round at a 10s window
WARMUP_SECONDS = 10

# Isolate this sweep from anything else sharing the same Kafka broker (e.g.
# another agent running a demo). Every child process gets its own topic prefix,
# checkpoint dir, and data dir, so resets and reports can't collide. The final
# plot/JSON below are still written to the real DATA_DIR alongside the other
# lesson artifacts.
RUN_CKPT_DIR = "ckpt-trig"
RUN_DATA_SUBDIR = "data/trigger_sweep_run"
RUN_ENV = {
    **os.environ,
    "L9_TOPIC_PREFIX": "trig-",
    "L9_CKPT_DIR": RUN_CKPT_DIR,
    "L9_DATA_DIR": RUN_DATA_SUBDIR,
}
RUN_DATA_DIR = ROOT / RUN_DATA_SUBDIR  # where the isolated child processes write


def reset_state() -> None:
    subprocess.run(
        [sys.executable, "src/setup_topics.py", "--reset"], cwd=ROOT, check=True, env=RUN_ENV
    )
    ckpt = ROOT / RUN_CKPT_DIR
    if ckpt.exists():
        shutil.rmtree(ckpt)


def run_round(trigger: int) -> dict:
    reset_state()
    # The final window can't close until the watermark (5s) advances past its
    # end and the next trigger fires, so drain has to outlast both.
    drain_seconds = 20 + trigger
    cmd = [
        sys.executable, "src/benchmark.py",
        "--rate", str(RATE),
        "--produce-duration", str(PRODUCE_DURATION),
        "--window-seconds", str(WINDOW_SECONDS),
        "--warmup-seconds", str(WARMUP_SECONDS),
        "--drain-seconds", str(drain_seconds),
        "--trigger", str(trigger),
    ]
    print(f"\n>>> trigger={trigger}s: {' '.join(cmd)}", file=sys.stderr)
    result = subprocess.run(cmd, cwd=ROOT, env=RUN_ENV)
    if result.returncode != 0:
        print(f"WARNING: benchmark run failed for trigger={trigger} (exit {result.returncode})", file=sys.stderr)

    report = json.loads((RUN_DATA_DIR / "latency_report.json").read_text())

    spark_windows = report["spark"]["windows"]
    flink_windows = report["flink"]["windows"]
    if spark_windows == 0 or flink_windows == 0:
        print(
            f"WARNING: trigger={trigger}s produced spark={spark_windows}, flink={flink_windows} "
            "windows — a zero here usually means a run collided on shared state. Not trustworthy.",
            file=sys.stderr,
        )
    else:
        print(
            f"    trigger={trigger}s: spark p50={report['spark']['processing_latency_ms']['p50']}ms "
            f"({spark_windows}w), flink p50={report['flink']['processing_latency_ms']['p50']}ms "
            f"({flink_windows}w)",
            file=sys.stderr,
        )

    # Per-round paper trail so a bad point can be traced back to its source.
    (DATA_DIR / f"trigger_sweep_{trigger}s.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )

    return {
        "trigger_seconds": trigger,
        "spark_windows": report["spark"]["windows"],
        "spark_p50_ms": report["spark"]["processing_latency_ms"]["p50"],
        "spark_p99_ms": report["spark"]["processing_latency_ms"]["p99"],
        "flink_windows": report["flink"]["windows"],
        "flink_p50_ms": report["flink"]["processing_latency_ms"]["p50"],
        "flink_p99_ms": report["flink"]["processing_latency_ms"]["p99"],
    }


def plot(results: list[dict], path: Path) -> None:
    triggers = [r["trigger_seconds"] for r in results]
    spark_p50_s = [r["spark_p50_ms"] / 1000 for r in results]
    flink_p50_s = [r["flink_p50_ms"] / 1000 for r in results]

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(triggers, spark_p50_s, marker="o", linewidth=2, label="Spark (p50 processing latency)")
    ax.plot(triggers, flink_p50_s, marker="o", linewidth=2, label="Flink (p50 processing latency)")
    ax.set_xlabel("Spark micro-batch trigger interval (s)")
    ax.set_ylabel("Median processing latency (s)")
    ax.set_title("Trigger interval vs. latency — measured on this machine")
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"chart saved to {path}")


def main():
    banner(
        "Lesson 9 trigger sweep",
        f"triggers:         {TRIGGERS}s",
        f"rate:             {RATE}/s (fixed)",
        f"window:           {WINDOW_SECONDS}s",
        f"produce duration: {PRODUCE_DURATION}s per trigger",
    )

    results = [run_round(t) for t in TRIGGERS]

    out_path = DATA_DIR / "trigger_sweep.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nsweep results saved to {out_path}")
    print(json.dumps(results, indent=2))

    plot(results, DATA_DIR / "trigger_sweep.png")


if __name__ == "__main__":
    main()
