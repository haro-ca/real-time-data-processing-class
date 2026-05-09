"""Slide 13 demo — Create lock contention on a single row.

Spawns N coroutines all UPDATEing the same row (id=1).
Most will queue on a row-level lock, visible in pg_stat_activity.

Usage:
    python demos/demo_locks.py [--connections 30]

Then in psql:
    SELECT pid, wait_event_type, wait_event,
           state, left(query, 40) AS query
    FROM pg_stat_activity
    WHERE wait_event_type = 'Lock';
"""

import argparse
import asyncio
import random

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"


async def ensure_target_row(pool: asyncpg.Pool) -> None:
    """Make sure row id=1 exists."""
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT id FROM orders WHERE id = 1")
        if row is None:
            await conn.execute(
                "INSERT INTO orders (id, customer_id, amount) "
                "OVERRIDING SYSTEM VALUE VALUES (1, 1, 10.00)")
            print("  ✓ Created target row (id=1)")
        else:
            print("  ✓ Target row (id=1) exists")


async def run(n: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=n, max_size=n)
    await ensure_target_row(pool)

    print(f"\nSpawning {n} coroutines all UPDATEing row id=1...")
    print("Switch to psql and run:\n")
    print("  SELECT pid, wait_event_type, wait_event,")
    print("         state, left(query, 40) AS query")
    print("  FROM pg_stat_activity")
    print("  WHERE wait_event_type = 'Lock';")
    print("\nPress Ctrl+C to stop.\n")

    running = True

    async def contender(idx: int):
        while running:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE orders SET amount = amount + $1 WHERE id = 1",
                        round(random.uniform(0.01, 1.0), 2),
                    )
            except (asyncpg.ConnectionDoesNotExistError, asyncpg.InterfaceError):
                break
            except Exception:
                await asyncio.sleep(0.01)

    tasks = [asyncio.create_task(contender(i)) for i in range(n)]

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        running = False
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pool.close()
        print(f"\n  ✓ Stopped {n} contenders.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slide 13 demo — lock contention")
    parser.add_argument("--connections", "-c", type=int, default=30)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.connections))
    except KeyboardInterrupt:
        print("\nDone.")
