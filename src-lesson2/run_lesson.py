"""Run all Lesson 2 demos in slide order, with pauses between sections.

Each demo maps to a specific slide. The script pauses between demos so
the instructor can narrate theory slides before continuing.

Usage:
    uv run python run_lesson.py                   # full lesson
    uv run python run_lesson.py --start 2         # skip benchmark, start at kill-node
    uv run python run_lesson.py --only 3          # run only distributed txn demo
    uv run python run_lesson.py --rows 50000      # benchmark row count
"""

import argparse
import asyncio
import os
import subprocess
import sys


BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
RESET = "\033[0m"


def banner(num: int, title: str, slide: str) -> None:
    width = 64
    print(flush=True)
    print(f"{CYAN}{'━' * width}{RESET}", flush=True)
    print(f"{CYAN}  Demo {num}{RESET}  {BOLD}{title}{RESET}", flush=True)
    print(f"{DIM}  Slide: {slide}{RESET}", flush=True)
    print(f"{CYAN}{'━' * width}{RESET}", flush=True)
    print(flush=True)


def pause(next_demo: str | None = None) -> None:
    if next_demo:
        msg = f"{YELLOW}▸ Next: {next_demo}. Press Enter to continue (q to quit)...{RESET}"
    else:
        msg = f"{GREEN}▸ Lesson complete. Press Enter to exit...{RESET}"
    try:
        response = input(msg)
        if response.strip().lower() == "q":
            print(f"\n{DIM}Exited by instructor.{RESET}")
            sys.exit(0)
    except (KeyboardInterrupt, EOFError):
        print(f"\n{DIM}Exited.{RESET}")
        sys.exit(0)


# ── Demo 1: Benchmark ──────────────────────────────────────────
async def demo_benchmark(rows: int) -> None:
    banner(1, "CockroachDB Benchmark (vs Lesson 1 Postgres)", "slide 03")
    print(f"{DIM}  Running the same scenarios from Lesson 1 against a 3-node cluster.{RESET}")
    print(f"{DIM}  Compare: same workload, same table, different engine.{RESET}")
    print()

    # Import and run the benchmark's main function
    from run_all import main as bench_main
    await bench_main(rows)


# ── Demo 2: Kill a node ────────────────────────────────────────
async def demo_kill_node(connections: int = 50, kill_after: float = 10, observe: float = 30) -> None:
    banner(2, "Kill a Node Under Load", "slide 11")
    print(f"{DIM}  Insert at steady-state, kill one node, observe recovery.{RESET}")
    print(f"{DIM}  Zero data loss — this is what you buy with the latency penalty.{RESET}")
    print()

    from demos.demo_kill_node import run as kill_run
    await kill_run(connections, kill_after, observe)


# ── Demo 3: Quorum progression ─────────────────────────────────
async def demo_quorum(connections: int = 20, phase_duration: float = 8) -> None:
    banner(3, "Quorum: the CP Guarantee in Action", "slide 12")
    print(f"{DIM}  3/3 → kill one → kill two (errors!) → restore one → restore all.{RESET}")
    print(f"{DIM}  Shows exactly when CP refuses writes to preserve consistency.{RESET}")
    print()

    from demos.demo_quorum import run as quorum_run
    await quorum_run(connections, phase_duration)


# ── Demo 4: Distributed transactions ───────────────────────────
async def demo_distributed_txn(transfers: int = 2000, connections: int = 10) -> None:
    banner(4, "Distributed Transactions — 2PC Overhead", "slide 22")
    print(f"{DIM}  Local vs cross-range transfers, low vs high contention.{RESET}")
    print(f"{DIM}  Measures the real cost of two-phase commit.{RESET}")
    print()

    from demos.demo_distributed_txn import run as txn_run
    await txn_run(transfers, connections)


# ── Demo 5: Latency injection ──────────────────────────────────
async def demo_latency_injection(rows: int) -> None:
    banner(5, "Latency Injection — Simulating Cross-Region", "slide 25")
    print(f"{DIM}  Baseline → inject 50ms → re-run → remove. All in one script.{RESET}")
    print()

    script = os.path.join(os.path.dirname(__file__), "demos", "demo_latency_injection.py")
    subprocess.run([sys.executable, script, "--rows", str(min(rows, 500))], check=True)


# ── Orchestrator ───────────────────────────────────────────────
DEMOS = [
    ("CockroachDB Benchmark", "slide 03"),
    ("Kill a Node Under Load", "slide 11"),
    ("Quorum: CP Guarantee", "slide 12"),
    ("Distributed Transactions (2PC)", "slide 22"),
    ("Latency Injection (Cross-Region)", "slide 25"),
]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Lesson 2: Run all demos in slide order")
    parser.add_argument("--rows", "-n", type=int, default=100_000,
                        help="Row count for benchmarks (default: 100000)")
    parser.add_argument("--start", "-s", type=int, default=1,
                        help="Start from demo N (1-5)")
    parser.add_argument("--only", "-o", type=int, default=None,
                        help="Run only demo N (1-5)")
    parser.add_argument("--continuous", "-c", action="store_true",
                        help="Run all demos back-to-back without pausing")
    args = parser.parse_args()

    print(f"\n{BOLD}{'═' * 64}{RESET}")
    print(f"{BOLD}  Lesson 2 — What happens when you distribute OLTP?{RESET}")
    print(f"{BOLD}{'═' * 64}{RESET}")
    print(f"\n{DIM}  Demos:{RESET}")
    for i, (name, slide) in enumerate(DEMOS, 1):
        marker = "→" if (args.only == i or (args.only is None and i >= args.start)) else " "
        print(f"  {DIM}{marker} {i}. {name:<40} ({slide}){RESET}")
    print()

    runners = [
        lambda: demo_benchmark(args.rows),
        lambda: demo_kill_node(),
        lambda: demo_quorum(),
        lambda: demo_distributed_txn(),
        lambda: demo_latency_injection(args.rows),
    ]

    if args.only:
        idx = args.only - 1
        if 0 <= idx < len(runners):
            await runners[idx]()
        else:
            print(f"{RED}Demo {args.only} doesn't exist. Choose 1-{len(runners)}.{RESET}")
        return

    for i, runner in enumerate(runners):
        if i + 1 < args.start:
            continue

        await runner()

        # Pause before next demo (unless --continuous)
        if not args.continuous:
            next_name = DEMOS[i + 1][0] if i + 1 < len(runners) else None
            pause(next_name)

    print(f"\n{GREEN}{BOLD}  ✓ All demos complete.{RESET}")
    print(f"{DIM}  Open http://localhost:8080 to explore the CockroachDB Admin UI.{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
