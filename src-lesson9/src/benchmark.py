"""End-to-end orchestrator for the Lesson 9 latency benchmark.

1. Starts Spark Structured Streaming and PyFlink DataStream first, and waits
   for both to report readiness (subscribed to Kafka) before any data flows.
   This matters: if the producer runs first and finishes before either engine
   starts, both pipelines cold-start against a pre-existing backlog and the
   measured "latency" is dominated by JVM/mini-cluster startup + backlog
   catch-up time instead of the steady-state processing behavior the lesson
   is about.
2. Warms up briefly, then produces orders into the now-live pipelines.
3. Drains after the producer finishes so the final window can close, then
   lets both pipelines self-stop via their own --max-time.
4. Runs the latency analyzer to produce CDFs and a JSON report.

Each step writes its own log to `data/*.log` so output is captured without
risking pipe-buffer deadlocks.

Usage:
    uv run python src/benchmark.py --rate 100 --produce-duration 240 --window-seconds 15
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from config import DATA_DIR, ROOT, banner

READY_TIMEOUT = 90  # seconds to wait for both pipelines to report ready before aborting


def log_path(name: str) -> Path:
    return DATA_DIR / f"{name}.log"


def run_script(name: str, *args: str, python: str | None = None) -> subprocess.Popen:
    interpreter = python or sys.executable
    cmd = [interpreter, f"src/{name}"] + list(args)
    log = log_path(name.replace(".py", ""))
    print(f"starting: {' '.join(cmd)}  ->  {log}", file=sys.stderr)
    DATA_DIR.mkdir(exist_ok=True)
    out = log.open("w")
    return subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)


def wait_for(proc: subprocess.Popen, timeout: int, label: str) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"{label} did not finish within {timeout}s; terminating", file=sys.stderr)
        proc.terminate()
        try:
            return proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait()


def wait_for_ready(names: list[str], timeout: int, poll: float = 0.5) -> list[str]:
    """Poll DATA_DIR for marker files; return the names still missing after timeout."""
    deadline = time.time() + timeout
    remaining = {n: DATA_DIR / n for n in names}
    while remaining and time.time() < deadline:
        for n, path in list(remaining.items()):
            if path.exists():
                del remaining[n]
        if remaining:
            time.sleep(poll)
    return list(remaining)


def main():
    parser = argparse.ArgumentParser(description="orchestrate L9 benchmark")
    parser.add_argument("--rate", type=int, default=50, help="events per second")
    parser.add_argument("--produce-duration", type=int, default=240, help="producer runtime in seconds")
    parser.add_argument("--window-seconds", type=int, default=15, help="tumbling window size")
    parser.add_argument("--trigger", type=int, default=2, help="Spark trigger interval in seconds")
    parser.add_argument(
        "--warmup-seconds", type=int, default=15,
        help="wait this long after both pipelines report ready before starting the producer",
    )
    parser.add_argument(
        "--drain-seconds", type=int, default=20,
        help="keep pipelines running this long after the producer finishes, so the final window can close",
    )
    parser.add_argument(
        "--flink-python",
        default=str(ROOT / ".venv-flink" / "bin" / "python"),
        help="Python interpreter to use for the PyFlink pipeline",
    )
    args = parser.parse_args()

    pipeline_max_time = args.warmup_seconds + args.produce_duration + args.drain_seconds
    expected_windows = args.produce_duration // args.window_seconds

    banner(
        "Lesson 9 benchmark orchestrator",
        f"window:           {args.window_seconds}s (~{expected_windows} windows expected per engine)",
        f"Spark trigger:    {args.trigger}s",
        f"warmup:           {args.warmup_seconds}s (after ready, before producing)",
        f"produce duration: {args.produce_duration}s at {args.rate}/s",
        f"drain:            {args.drain_seconds}s (after producing, before stopping)",
    )
    if expected_windows < 10:
        print(
            f"WARNING: only ~{expected_windows} windows expected; percentiles need more "
            "samples to be meaningful. Increase --produce-duration or lower --window-seconds.",
            file=sys.stderr,
        )

    print("\nlogs written to:", file=sys.stderr)
    for n in ("spark_pipeline", "flink_pipeline", "producer", "analyze_latency"):
        print(f"  {log_path(n)}", file=sys.stderr)
    print(file=sys.stderr)

    # 0. Clear stale readiness markers from a previous run.
    ready_names = ["spark.ready", "flink.ready"]
    for n in ready_names:
        (DATA_DIR / n).unlink(missing_ok=True)

    # 1. Start both pipelines FIRST — see module docstring for why.
    spark = run_script(
        "spark_pipeline.py",
        "--trigger", str(args.trigger),
        "--max-time", str(pipeline_max_time),
        "--window-seconds", str(args.window_seconds),
    )
    flink = run_script(
        "flink_pipeline.py",
        "--max-time", str(pipeline_max_time),
        "--window-seconds", str(args.window_seconds),
        python=args.flink_python,
    )

    print("waiting for both pipelines to report ready...", file=sys.stderr)
    not_ready = wait_for_ready(ready_names, timeout=READY_TIMEOUT)
    if not_ready:
        print(f"pipelines never became ready: {not_ready}; aborting", file=sys.stderr)
        spark.terminate()
        flink.terminate()
        sys.exit(1)
    print(f"both pipelines ready; warming up for {args.warmup_seconds}s...", file=sys.stderr)
    time.sleep(args.warmup_seconds)

    # 2. Produce events into the now-live pipelines.
    producer = run_script(
        "producer.py",
        "--rate", str(args.rate),
        "--duration-seconds", str(args.produce_duration),
    )
    if wait_for(producer, args.produce_duration + 60, "producer") != 0:
        print("producer failed", file=sys.stderr)
        spark.terminate()
        flink.terminate()
        sys.exit(1)
    print(f"\nproducer finished; draining for {args.drain_seconds}s so the final window can close...")

    # 3. Drain, then wait for both pipelines to self-stop via --max-time.
    time.sleep(args.drain_seconds)
    spark_rc = wait_for(spark, 90, "Spark pipeline")
    flink_rc = wait_for(flink, 90, "Flink pipeline")

    for label, rc in [("Spark", spark_rc), ("Flink", flink_rc)]:
        if rc not in (0, -9, -15):
            print(f"{label} pipeline exited with code {rc}", file=sys.stderr)

    print("\npipelines finished; analyzing latency...")

    # 4. Analyze results.
    analyzer = run_script("analyze_latency.py")
    if wait_for(analyzer, 120, "analyzer") != 0:
        print("analyze_latency failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
