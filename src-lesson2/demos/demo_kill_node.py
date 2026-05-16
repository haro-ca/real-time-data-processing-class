"""Demo — Kill a CockroachDB node mid-workload, measure what happens.

Inserts rows at steady-state, kills one node, observes the latency spike
and recovery, then verifies zero data loss. Compare with Lesson 1's
demo_sync_loss.py where Postgres *lost* data.

Usage:
    python demos/demo_kill_node.py [--connections 50] [--kill-after 10] [--observe 20]

    --kill-after  seconds of steady-state before killing a node
    --observe     total seconds to run (including post-kill)
"""

import argparse
import asyncio
import subprocess
import time

import asyncpg

DSN = "postgresql://root@localhost:26257/bench?sslmode=disable"
INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"
CONTAINERS = ["lesson2-crdb-1", "lesson2-crdb-2", "lesson2-crdb-3"]
KILL_TARGET = "lesson2-crdb-3"  # kill the non-SQL-port node


async def run(connections: int, kill_after: float, observe: float) -> None:
    import random

    print("Demo — Node failure under load")
    print("=" * 60)

    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)

    # Clean slate so row-count comparison is accurate
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE orders")

    # Tracking
    stats = {
        "total": 0,
        "errors": 0,
        "phase": "pre-kill",
        "tps_pre": [],
        "tps_post": [],
    }
    running = True
    node_killed = False
    kill_time = None

    async def worker():
        while running:
            try:
                async with pool.acquire() as conn:
                    while running:
                        await conn.execute(
                            INSERT_SQL,
                            random.randint(1, 10_000),
                            round(random.uniform(1, 500), 2),
                        )
                        stats["total"] += 1
            except (
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                asyncpg.InternalServerError,
                asyncpg.PostgresError,
                OSError,
            ):
                stats["errors"] += 1
                if running:
                    await asyncio.sleep(0.1)  # back off before reconnect

    # Reporter
    async def reporter():
        nonlocal node_killed
        t0 = time.monotonic()
        last_total = 0
        last_time = t0

        while running:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            elapsed = now - t0
            interval_rows = stats["total"] - last_total
            interval_secs = now - last_time
            tps = interval_rows / interval_secs if interval_secs > 0 else 0

            marker = ""
            if node_killed:
                if kill_time and (now - kill_time) < 2:
                    marker = "  ← NODE KILLED"
                stats["tps_post"].append(tps)
            else:
                stats["tps_pre"].append(tps)

            print(f"    [{elapsed:5.1f}s]  {tps:>8,.0f} TPS  |  errors: {stats['errors']:>4}  |  {stats['phase']}{marker}")
            last_total = stats["total"]
            last_time = now

    # Start workers + reporter
    tasks = [asyncio.create_task(worker()) for _ in range(connections)]
    reporter_task = asyncio.create_task(reporter())

    # Phase 1: steady state
    print(f"\n  Phase 1: Steady state ({kill_after:.0f}s, {connections} connections)")
    print(f"  Raft consensus on every commit — compare with Lesson 1 Postgres numbers.\n")
    await asyncio.sleep(kill_after)

    # Phase 2: kill a node
    print(f"\n  💥 Killing node '{KILL_TARGET}'...")
    stats["phase"] = "post-kill"
    result = subprocess.run(
        ["docker", "kill", KILL_TARGET],
        capture_output=True, text=True,
    )
    node_killed = True
    kill_time = time.monotonic()
    if result.returncode == 0:
        print("  ✓ Node killed. Cluster has 2 of 3 nodes — still has quorum.\n")
    else:
        print(f"  ⚠ docker kill failed: {result.stderr.strip()}")

    # Phase 3: observe recovery
    remaining = observe - kill_after
    print(f"  Phase 2: Observing recovery ({remaining:.0f}s remaining)\n")
    await asyncio.sleep(remaining)

    # Stop — let workers finish their current insert before exiting
    running = False
    await asyncio.gather(*tasks, return_exceptions=True)
    reporter_task.cancel()
    await asyncio.gather(reporter_task, return_exceptions=True)

    # Count what's in the database
    conn = await asyncpg.connect(DSN)
    db_count = await conn.fetchval("SELECT count(*) FROM orders")
    await conn.close()

    # Results
    avg_pre = sum(stats["tps_pre"]) / len(stats["tps_pre"]) if stats["tps_pre"] else 0
    avg_post = sum(stats["tps_post"]) / len(stats["tps_post"]) if stats["tps_post"] else 0

    print("\n" + "=" * 60)
    print("  Results:")
    print(f"    Pre-kill avg TPS:     {avg_pre:>10,.0f}")
    print(f"    Post-kill avg TPS:    {avg_post:>10,.0f}")
    print(f"    TPS ratio:            {avg_post / avg_pre:>10.2f}×" if avg_pre > 0 else "")
    print(f"    Total errors:         {stats['errors']:>10,}")
    print(f"    Client-side inserts:  {stats['total']:>10,}")
    print(f"    Database row count:   {db_count:>10,}")
    diff = db_count - stats["total"]
    if diff >= 0:
        print(f"\n  ✓ ZERO data loss. Every acknowledged row survived.")
        if diff > 0:
            print(f"    ({diff} extra rows committed mid-cancel before counter incremented)")
        print("    This is what you're buying with the latency penalty.")
    else:
        print(f"\n  ⚠ {-diff:,} rows missing — acknowledged by client but not in DB")
    print("=" * 60)

    # Restart the killed node
    print(f"\n  Restarting '{KILL_TARGET}'...")
    subprocess.run(["docker", "start", KILL_TARGET], capture_output=True, text=True)
    print("  ✓ Node restarted. It will catch up via Raft log replay.")
    print("  Check http://localhost:8080/#/metrics/overview to watch it rejoin.\n")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo — kill a CockroachDB node under load")
    parser.add_argument("--connections", "-c", type=int, default=50)
    parser.add_argument("--kill-after", type=float, default=10,
                        help="Seconds of steady-state before killing a node")
    parser.add_argument("--observe", type=float, default=30,
                        help="Total observation time in seconds")
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.kill_after, args.observe))
