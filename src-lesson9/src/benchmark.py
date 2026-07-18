"""End-to-end orchestrator for the Lesson 9 latency benchmark.

1. Produces orders to Kafka.
2. Runs Spark Structured Streaming and PyFlink DataStream concurrently.
3. Waits for both pipelines to finish.
4. Runs the latency analyzer to produce CDFs and a JSON report.

Each step writes its own log to `data/*.log` so output is captured without
risking pipe-buffer deadlocks.

Usage:
    uv run python src/benchmark.py --rate 100 --produce-duration 600 --pipeline-runtime 900
"""

import argparse
import subprocess
import sys
from pathlib import Path

from config import DATA_DIR, ROOT, banner


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


def main():
    parser = argparse.ArgumentParser(description="orchestrate L9 benchmark")
    parser.add_argument("--rate", type=int, default=100, help="events per second")
    parser.add_argument("--produce-duration", type=int, default=600, help="producer runtime in seconds")
    parser.add_argument("--pipeline-runtime", type=int, default=900, help="how long to let each pipeline run")
    parser.add_argument("--trigger", type=int, default=2, help="Spark trigger interval in seconds")
    parser.add_argument("--window-seconds", type=int, default=300, help="tumbling window size")
    parser.add_argument(
        "--flink-python",
        default=str(ROOT / ".venv-flink" / "bin" / "python"),
        help="Python interpreter to use for the PyFlink pipeline",
    )
    args = parser.parse_args()

    banner(
        "Lesson 9 benchmark orchestrator",
        f"produce duration: {args.produce_duration}s at {args.rate}/s",
        f"pipeline runtime: {args.pipeline_runtime}s",
        f"window:           {args.window_seconds}s",
        f"Spark trigger:    {args.trigger}s",
    )

    print("\nlogs written to:", file=sys.stderr)
    for n in ("producer", "spark_pipeline", "flink_pipeline", "analyze_latency"):
        print(f"  {log_path(n)}", file=sys.stderr)
    print(file=sys.stderr)

    # 1. Produce events (sequential).
    producer = run_script(
        "producer.py",
        "--rate", str(args.rate),
        "--duration-seconds", str(args.produce_duration),
    )
    if wait_for(producer, args.produce_duration + 60, "producer") != 0:
        print("producer failed", file=sys.stderr)
        sys.exit(1)
    print("\nproducer finished; starting pipelines...")

    # 2. Run both pipelines concurrently.
    spark = run_script(
        "spark_pipeline.py",
        "--trigger", str(args.trigger),
        "--max-time", str(args.pipeline_runtime),
        "--window-seconds", str(args.window_seconds),
    )
    flink = run_script(
        "flink_pipeline.py",
        "--max-time", str(args.pipeline_runtime),
        "--window-seconds", str(args.window_seconds),
        python=args.flink_python,
    )

    spark_rc = wait_for(spark, args.pipeline_runtime + 120, "Spark pipeline")
    flink_rc = wait_for(flink, args.pipeline_runtime + 120, "Flink pipeline")

    for label, rc in [("Spark", spark_rc), ("Flink", flink_rc)]:
        if rc not in (0, -9, -15):
            print(f"{label} pipeline exited with code {rc}", file=sys.stderr)

    print("\npipelines finished; analyzing latency...")

    # 3. Analyze results.
    analyzer = run_script("analyze_latency.py")
    if wait_for(analyzer, 120, "analyzer") != 0:
        print("analyze_latency failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
