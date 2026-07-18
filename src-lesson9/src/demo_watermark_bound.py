"""Demo: Flink's latency floor tracks the watermark bound, not a trigger interval.

Flink-only (no Spark) — sweeps --watermark-seconds across a few values and
shows the measured processing latency moves with it, roughly 1:1. This
isolates the mechanism slide 4 claims (watermark delay dominates Flink's
latency floor) directly, instead of inferring it from a Spark-vs-Flink
comparison where other factors are also in play.

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

from benchmark import READY_TIMEOUT, run_script, wait_for, wait_for_ready
from config import DATA_DIR, ROOT, banner

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


def run_round(watermark_seconds: int) -> dict:
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

    analyzer = run_script("analyze_latency.py")
    wait_for(analyzer, 60, "analyzer")

    report = json.loads((DATA_DIR / "latency_report.json").read_text())
    (DATA_DIR / f"watermark_demo_{watermark_seconds}s.json").write_text(json.dumps(report, indent=2) + "\n")

    return {
        "watermark_seconds": watermark_seconds,
        "flink_windows": report["flink"]["windows"],
        "flink_p50_ms": report["flink"]["processing_latency_ms"]["p50"],
        "flink_p99_ms": report["flink"]["processing_latency_ms"]["p99"],
    }


def plot(results: list[dict], path: Path) -> None:
    watermarks = [r["watermark_seconds"] for r in results]
    p50_s = [r["flink_p50_ms"] / 1000 for r in results]

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(watermarks, p50_s, marker="o", linewidth=2, color="#5cd6e8", label="Flink p50 processing latency")
    ax.plot(watermarks, watermarks, linestyle="--", color="#67737f", label="y = x (watermark bound alone)")
    ax.set_xlabel("configured watermark bound (s)")
    ax.set_ylabel("measured processing latency (s)")
    ax.set_title("Flink's latency floor tracks the watermark bound")
    ax.legend()
    ax.grid(True, alpha=0.3)
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

    results = [run_round(w) for w in WATERMARKS_SECONDS]

    out_path = DATA_DIR / "watermark_bound_demo.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nresults saved to {out_path}")
    print(json.dumps(results, indent=2))

    plot(results, DATA_DIR / "watermark_bound_demo.png")


if __name__ == "__main__":
    main()
