"""Extend trigger_sweep.py's data with new trigger points.

trigger_sweep.py measured [1, 2, 5, 10]s and found Spark and Flink nearly
tied at trigger=1s. This answers the natural follow-up: does Spark's floor
keep falling toward Flink's below 1s, or does it hit a wall first? Reuses
trigger_sweep's own run_round()/plot() so the methodology (topic isolation,
warmup/drain, reset between rounds) is identical. Safely re-runnable — any
trigger already present in data/trigger_sweep.json is skipped, not re-run.

Edit NEW_TRIGGERS below and re-run to push further.

Usage:
    uv run python src/trigger_sweep_extend.py
"""

import json
import sys

import matplotlib.pyplot as plt

from config import DATA_DIR, banner
from trigger_sweep import plot, run_round

NEW_TRIGGERS = [0.01]  # 10ms


def plot_log(results: list[dict], path) -> None:
    """Same data as trigger_sweep.plot(), log-scale x-axis.

    A linear axis crams every point below 1s into an unreadable sliver near
    zero, hiding exactly the part of the curve this extension exists to
    show — that Spark's floor flattens hard at 5.0s from 10ms to 250ms,
    25x of trigger range with zero further improvement. Log scale spreads
    that out instead of the (uninteresting, already well covered) 1s-10s
    climb dominating the plot.
    """
    triggers = [r["trigger_seconds"] for r in results]
    spark_p50_s = [r["spark_p50_ms"] / 1000 for r in results]
    flink_p50_s = [r["flink_p50_ms"] / 1000 for r in results]

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(triggers, spark_p50_s, marker="o", linewidth=2, color="#5cd6e8", label="Spark (p50 processing latency)")
    ax.plot(triggers, flink_p50_s, marker="o", linewidth=2, color="#ff6e54", label="Flink (p50 processing latency)")
    ax.axhline(5.0, color="#67737f", linestyle="--", linewidth=1.5, label="5s watermark (shared floor)")
    ax.set_xscale("log")
    ax.set_xlabel("Spark micro-batch trigger interval (s, log scale)")
    ax.set_ylabel("Median processing latency (s)")
    ax.set_title("Trigger interval vs. latency, log scale — the floor, found")
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"log-scale chart saved to {path}")


def main():
    existing = json.loads((DATA_DIR / "trigger_sweep.json").read_text())
    existing_triggers = {r["trigger_seconds"] for r in existing}
    to_run = [t for t in NEW_TRIGGERS if t not in existing_triggers]

    banner(
        "Trigger sweep extension",
        f"requested: {NEW_TRIGGERS}",
        f"already measured, skipping: {sorted(existing_triggers & set(NEW_TRIGGERS))}",
        f"new points to run: {to_run}",
    )

    if not to_run:
        print("nothing new to run", file=sys.stderr)
        return

    new_results = [run_round(t) for t in to_run]

    combined = sorted(existing + new_results, key=lambda r: r["trigger_seconds"])

    out_path = DATA_DIR / "trigger_sweep.json"
    out_path.write_text(json.dumps(combined, indent=2) + "\n")
    print(f"\ncombined results saved to {out_path}")
    print(json.dumps(combined, indent=2))

    plot(combined, DATA_DIR / "trigger_sweep.png")
    plot_log(combined, DATA_DIR / "trigger_sweep_log.png")


if __name__ == "__main__":
    main()
