"""Experiment C — Hot row: all coroutines UPDATE the same row, demonstrating lock collapse.

Usage:
    python experiment_hotrow.py [--connections 50] [--duration 30]

Compare with:
    python load_async.py --connections 50 --mode insert
to see the dramatic TPS difference.
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"


async def ensure_target_row(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT id FROM orders WHERE id = 1")
        if row is None:
            await conn.execute(
                "INSERT INTO orders (id, customer_id, amount) "
                "OVERRIDING SYSTEM VALUE VALUES (1, 1, 10.00)")


async def run(connections: int, duration: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    await ensure_target_row(pool)

    print(f"Experiment C — Hot row: {connections} connections, all UPDATE id=1, {duration}s")
    print("-" * 60)

    done = 0
    done_lock = asyncio.Lock()
    running = True
    t0 = time.monotonic()

    async def reporter():
        nonlocal done
        while running:
            await asyncio.sleep(1.0)
            elapsed = time.monotonic() - t0
            async with done_lock:
                current = done
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {current:,}")

    async def contender():
        nonlocal done
        while running:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE orders SET amount = amount + $1 WHERE id = 1",
                        round(random.uniform(0.01, 1.0), 2),
                    )
                async with done_lock:
                    done += 1
            except Exception:
                await asyncio.sleep(0.01)

    reporter_task = asyncio.create_task(reporter())
    tasks = [asyncio.create_task(contender()) for _ in range(connections)]

    await asyncio.sleep(duration)
    running = False

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    reporter_task.cancel()

    elapsed = time.monotonic() - t0
    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} updates in {elapsed:.1f}s → {tps:,.0f} TPS")
    print("Compare this to INSERT throughput at the same concurrency level.")
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment C — Hot row contention")
    parser.add_argument("--connections", "-c", type=int, default=50)
    parser.add_argument("--duration", "-d", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.duration))
