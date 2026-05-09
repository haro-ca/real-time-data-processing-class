"""ANNEX — Demonstrate work_mem spill-to-disk vs in-memory sort.

Shows the same query running with tiny work_mem (spills to disk temp files)
vs large work_mem (sorts in memory). Prints EXPLAIN ANALYZE output for both.

Usage:
    python demos/demo_work_mem.py [--rows 100000]

Requires the orders table to have data (run load_naive.py first).
"""

import argparse
import asyncio

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"

SORT_QUERY = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT * FROM orders ORDER BY amount DESC"


async def run(rows: int) -> None:
    conn = await asyncpg.connect(DSN)

    # Ensure enough data
    count = await conn.fetchval("SELECT count(*) FROM orders")
    if count < rows:
        print(f"  Need at least {rows:,} rows. Current: {count:,}.")
        print(f"  Run: uv run python load_naive.py --rows {rows}")
        await conn.close()
        return

    print(f"ANNEX — work_mem demo ({count:,} rows in orders table)")
    print("=" * 70)

    # Test 1: tiny work_mem (forces disk spill)
    print("\n┌─ work_mem = 64kB (spill to disk)")
    print("└" + "─" * 69)
    await conn.execute("SET work_mem = '64kB'")
    plan = await conn.fetch(SORT_QUERY)
    for row in plan:
        print(f"  {row[0]}")

    # Test 2: large work_mem (in-memory sort)
    print("\n┌─ work_mem = 256MB (in-memory sort)")
    print("└" + "─" * 69)
    await conn.execute("SET work_mem = '256MB'")
    plan = await conn.fetch(SORT_QUERY)
    for row in plan:
        print(f"  {row[0]}")

    print("\n" + "=" * 70)
    print("Key difference:")
    print("  • 64kB  → 'Sort Method: external merge  Disk: ...'  (slow)")
    print("  • 256MB → 'Sort Method: quicksort  Memory: ...'     (fast)")
    print("\nIn production: work_mem × active_connections × sorts_per_query = total memory risk")

    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ANNEX — work_mem spill demo")
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    args = parser.parse_args()
    asyncio.run(run(args.rows))
