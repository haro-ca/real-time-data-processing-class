"""Demo: without with_idleness(), a quiet Kafka partition stalls Flink's
watermark forever — pending windows never close.

Produces a short burst, then goes silent. Runs Flink twice: once with
idleness detection enabled (the default in flink_pipeline.py) and once with
it disabled (--disable-idleness). Compares whether the trailing window — the
one still open when the source goes quiet — ever closes.

This is a real, common Flink gotcha: a bounded-out-of-orderness watermark
only advances when new records arrive. If the source goes quiet, the
watermark freezes wherever it was, and any window waiting on it never fires
— silently, with no error.

Usage:
    uv run python src/demo_idle_source_stall.py
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from benchmark import READY_TIMEOUT, run_script, wait_for, wait_for_ready
from config import DATA_DIR, ROOT, banner

WINDOW_SECONDS = 10
WATERMARK_SECONDS = 3
BURST_DURATION = 20   # 2 full windows worth of data: [0,10) and [10,20)
WARMUP_SECONDS = 10
OBSERVE_SECONDS = 30  # silence after the burst, watching for the trailing window to close


def reset_state() -> None:
    subprocess.run([sys.executable, "src/setup_topics.py", "--reset"], cwd=ROOT, check=True)
    ckpt = ROOT / "ckpt"
    if ckpt.exists():
        shutil.rmtree(ckpt)


def run_scenario(disable_idleness: bool) -> dict:
    reset_state()
    (DATA_DIR / "flink.ready").unlink(missing_ok=True)

    flink_python = str(ROOT / ".venv-flink" / "bin" / "python")
    pipeline_max_time = WARMUP_SECONDS + BURST_DURATION + OBSERVE_SECONDS
    args = [
        "--max-time", str(pipeline_max_time),
        "--window-seconds", str(WINDOW_SECONDS),
        "--watermark-seconds", str(WATERMARK_SECONDS),
    ]
    if disable_idleness:
        args.append("--disable-idleness")
    flink = run_script("flink_pipeline.py", *args, python=flink_python)

    label = "idleness DISABLED" if disable_idleness else "idleness enabled"
    print(f"\n>>> {label}: waiting for Flink to report ready...", file=sys.stderr)
    not_ready = wait_for_ready(["flink.ready"], timeout=READY_TIMEOUT)
    if not_ready:
        flink.terminate()
        raise RuntimeError(f"Flink never became ready ({label})")

    time.sleep(WARMUP_SECONDS)

    producer = run_script("producer.py", "--rate", "50", "--duration-seconds", str(BURST_DURATION))
    wait_for(producer, BURST_DURATION + 60, "producer")

    print(
        f"burst finished; source now silent for {OBSERVE_SECONDS}s, watching for the trailing window...",
        file=sys.stderr,
    )
    time.sleep(OBSERVE_SECONDS)
    wait_for(flink, 60, "Flink pipeline")

    analyzer = run_script("analyze_latency.py")
    wait_for(analyzer, 60, "analyzer")

    report = json.loads((DATA_DIR / "latency_report.json").read_text())
    suffix = "disabled" if disable_idleness else "enabled"
    (DATA_DIR / f"idle_demo_{suffix}.json").write_text(json.dumps(report, indent=2) + "\n")

    expected_windows = BURST_DURATION // WINDOW_SECONDS
    windows_closed = report["flink"]["windows"]
    return {
        "idleness_enabled": not disable_idleness,
        "windows_closed": windows_closed,
        "expected_windows": expected_windows,
        "trailing_window_closed": windows_closed >= expected_windows,
        "flink_p50_ms": report["flink"]["processing_latency_ms"]["p50"],
        "flink_max_ms": report["flink"]["processing_latency_ms"]["max"],
    }


def main():
    banner(
        "Idle-source watermark-stall demo (Flink only)",
        f"window:    {WINDOW_SECONDS}s, watermark: {WATERMARK_SECONDS}s",
        f"burst:     {BURST_DURATION}s of data, then {OBSERVE_SECONDS}s of silence",
    )

    results = [run_scenario(disable_idleness=False), run_scenario(disable_idleness=True)]

    out_path = DATA_DIR / "idle_source_stall_demo.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nresults saved to {out_path}")
    print(json.dumps(results, indent=2))

    for r in results:
        label = "WITH idleness" if r["idleness_enabled"] else "WITHOUT idleness"
        verdict = "closed" if r["trailing_window_closed"] else "STALLED — never closed"
        print(f"{label}: {r['windows_closed']}/{r['expected_windows']} expected windows closed -> trailing window {verdict}")


if __name__ == "__main__":
    main()
