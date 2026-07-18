"""Demo: Flink's latency floor tracks the watermark bound, not a trigger interval.

Flink-only (no Spark) — sweeps --watermark-seconds across a few values and
shows each individual closed window's measured latency as a bar against a
dashed reference line at the configured watermark bound. This deliberately
mirrors demo_trigger_floor.py's visual grammar (bars vs. a fixed reference
line, one panel per condition) so the two "proof" slides read the same way:
bars sitting just above the line means the floor is pinned to whatever the
line represents.

An earlier version of this demo plotted a single p50 point per watermark
against a y=x line — it technically showed latency increasing with the
watermark, but the fit was sloppy (1s->3.1s, 5s->5.5s, 10s->12.2s isn't
linear) because a single summary point per round hides how few windows each
round closes and doesn't make the mechanism legible. Per-window bars fix
both problems.

Usage:
    uv run python src/demo_watermark_bound.py
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt

from analyze_latency import compute_latencies, drain_topic
from benchmark import READY_TIMEOUT, run_script, wait_for, wait_for_ready
from config import BOOTSTRAP, DATA_DIR, FLINK_RESULTS_TOPIC, ROOT, banner

WATERMARKS_SECONDS = [1, 5, 10]
WINDOW_SECONDS = 10
PRODUCE_DURATION = 60
WARMUP_SECONDS = 10
DRAIN_SECONDS = 15


def reset_state() -> None:
    subprocess.run([sys.executable, "src/setup_topics.py", "--reset"], cwd=ROOT, check=True)
    ckpt = ROOT / "ckpt"
    if ckpt.exists():
        shutil.rmtree(ckpt)


def run_round(watermark_seconds: int) -> list[float]:
    """Returns the raw per-window processing latency (ms) for this watermark setting."""
    reset_state()
    (DATA_DIR / "flink.ready").unlink(missing_ok=True)

    flink_python = str(ROOT / ".venv-flink" / "bin" / "python")
    pipeline_max_time = WARMUP_SECONDS + PRODUCE_DURATION + DRAIN_SECONDS
    flink = run_script(
        "flink_pipeline.py",
        "--max-time", str(pipeline_max_time),
        "--window-seconds", str(WINDOW_SECONDS),
        "--watermark-seconds", str(watermark_seconds),
        python=flink_python,
    )

    print(f"\n>>> watermark={watermark_seconds}s: waiting for Flink to report ready...", file=sys.stderr)
    not_ready = wait_for_ready(["flink.ready"], timeout=READY_TIMEOUT)
    if not_ready:
        flink.terminate()
        raise RuntimeError(f"Flink never became ready for watermark={watermark_seconds}s")

    time.sleep(WARMUP_SECONDS)

    producer = run_script("producer.py", "--rate", "50", "--duration-seconds", str(PRODUCE_DURATION))
    wait_for(producer, PRODUCE_DURATION + 60, "producer")

    time.sleep(DRAIN_SECONDS)
    wait_for(flink, 90, "Flink pipeline")

    records = drain_topic(FLINK_RESULTS_TOPIC, BOOTSTRAP)
    processing, _ = compute_latencies(records)
    processing_ms = sorted(float(x) for x in processing)

    (DATA_DIR / f"watermark_demo_{watermark_seconds}s.json").write_text(
        json.dumps({"watermark_seconds": watermark_seconds, "processing_latency_ms": processing_ms}, indent=2) + "\n"
    )
    return processing_ms


def plot(results: dict[int, list[float]], path: Path) -> None:
    # Shared y-axis on purpose: each panel auto-scaling independently made
    # every panel's bars fill the same visual space regardless of actual
    # magnitude, hiding the real story (latency growing across panels). A
    # shared axis makes the 1s panel's bars visibly short next to the 10s
    # panel's tall ones. Per-window variation here is genuinely tiny (a few
    # ms out of several thousand — Flink is very deterministic under this
    # steady, uncontended load), so value labels make the real numbers
    # legible even though the bars look almost flat within a panel.
    y_max = max(max(d) for d in results.values()) * 1.2

    fig, axes = plt.subplots(1, len(results), figsize=(5.5 * len(results), 5), sharey=True)
    for ax, (watermark_seconds, durations) in zip(axes, results.items()):
        line_ms = watermark_seconds * 1000
        bars = ax.bar(range(len(durations)), durations, color="#5cd6e8")
        ax.bar_label(bars, labels=[f"{d:,.0f}" for d in durations], padding=3, fontsize=8)
        ax.axhline(line_ms, color="#ff6e54", linestyle="--", linewidth=2, label=f"watermark = {watermark_seconds}s")
        ax.set_ylim(0, y_max)
        ax.set_title(f"watermark = {watermark_seconds}s")
        ax.set_xlabel("window #")
        ax.set_ylabel("measured processing latency (ms)")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
    fig.suptitle("Flink's latency floor tracks the watermark bound (shared y-axis across panels)")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"chart saved to {path}")


def main():
    banner(
        "Watermark bound vs. latency demo (Flink only)",
        f"watermarks:       {WATERMARKS_SECONDS}",
        f"window:           {WINDOW_SECONDS}s",
        f"produce duration: {PRODUCE_DURATION}s per round",
    )

    results = {w: run_round(w) for w in WATERMARKS_SECONDS}

    out_path = DATA_DIR / "watermark_bound_demo.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nresults saved to {out_path}")
    for w, durations in results.items():
        avg = sum(durations) / len(durations) if durations else 0
        print(f"  watermark={w}s: {len(durations)} windows, avg latency {avg:.0f}ms")

    plot(results, DATA_DIR / "watermark_bound_demo.png")


if __name__ == "__main__":
    main()
