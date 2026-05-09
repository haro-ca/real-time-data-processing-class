"""Slide 5 demo — Open N connections so you can inspect pg_stat_activity.

Opens connections in different states (idle, active with a slow query)
and holds them open until you press Ctrl+C.

Usage:
    python demos/demo_connections.py [--connections 20]

Then in psql:
    SELECT pid, state, wait_event_type
    FROM pg_stat_activity
    WHERE backend_type = 'client backend';
"""

import argparse
import asyncio
import random

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"


async def run(n: int) -> None:
    print(f"Opening {n} connections to Postgres...")
    conns: list[asyncpg.Connection] = []

    for i in range(n):
        conn = await asyncpg.connect(DSN)
        conns.append(conn)

    print(f"  ✓ {n} connections open\n")
    print("Some connections will run slow queries to show 'active' state.")
    print("Switch to psql and run:\n")
    print("  SELECT pid, state, wait_event_type, left(query, 50) AS query")
    print("  FROM pg_stat_activity")
    print("  WHERE backend_type = 'client backend';")
    print("\nPress Ctrl+C to close all connections.\n")

    async def slow_query(conn: asyncpg.Connection, idx: int):
        """Run pg_sleep in a loop to keep some connections 'active'."""
        while True:
            try:
                await conn.execute(f"SELECT pg_sleep({random.uniform(0.5, 2.0):.1f})")
                await asyncio.sleep(random.uniform(0.1, 0.5))
            except (asyncpg.ConnectionDoesNotExistError, asyncpg.InterfaceError):
                break

    tasks = []
    active_count = max(1, n // 4)
    for i in range(active_count):
        tasks.append(asyncio.create_task(slow_query(conns[i], i)))

    print(f"  {active_count} connections running slow queries (active)")
    print(f"  {n - active_count} connections idle")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        for conn in conns:
            try:
                await conn.close()
            except Exception:
                pass
        print(f"\n  ✓ Closed {n} connections.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slide 5 demo — connection overhead")
    parser.add_argument("--connections", "-c", type=int, default=20)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.connections))
    except KeyboardInterrupt:
        print("\nDone.")
