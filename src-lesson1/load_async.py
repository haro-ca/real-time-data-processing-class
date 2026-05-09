"""Phase 2 — Async: asyncpg connection pool with N concurrent coroutines.

Usage:
    python load_async.py [--connections 50] [--rows 100000] [--mode insert] [--no-sync]

Modes:
    insert  — INSERT new rows (default)
    update  — UPDATE random existing rows (needs pre-loaded data)

Flags:
    --no-sync  — SET synchronous_commit = off on each connection (Experiment B)
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"

INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"
UPDATE_SQL = "UPDATE orders SET amount = amount + $1 WHERE id = $2"


async def run(connections: int, total_rows: int, mode: str, no_sync: bool) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)

    if no_sync:
        async with pool.acquire() as conn:
            await conn.execute("SET synchronous_commit = off")
        print("  ⚠ synchronous_commit = off (session-level on pool init connection)")

    label = f"Phase 2 — Async loader: {total_rows:,} rows, {connections} connections, mode={mode}"
    if no_sync:
        label += ", sync_commit=off"
    print(label)
    print("-" * 60)

    done = 0
    done_lock = asyncio.Lock()
    t0 = time.monotonic()

    async def reporter():
        nonlocal done
        while True:
            await asyncio.sleep(1.0)
            elapsed = time.monotonic() - t0
            async with done_lock:
                current = done
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {current:,}")
            if current >= total_rows:
                break

    sem = asyncio.Semaphore(connections * 4)

    async def worker(i: int):
        nonlocal done
        async with sem:
            async with pool.acquire() as conn:
                if no_sync:
                    await conn.execute("SET synchronous_commit = off")
                if mode == "insert":
                    await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                else:
                    row_id = random.randint(1, max(1, total_rows // 2))
                    await conn.execute(UPDATE_SQL, round(random.uniform(0.01, 1.0), 2), row_id)
            async with done_lock:
                done += 1

    reporter_task = asyncio.create_task(reporter())
    tasks = [asyncio.create_task(worker(i)) for i in range(total_rows)]
    await asyncio.gather(*tasks)
    await reporter_task

    elapsed = time.monotonic() - t0
    tps = total_rows / elapsed
    print("-" * 60)
    print(f"Done. {total_rows:,} rows in {elapsed:.1f}s → {tps:,.0f} TPS")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 2 — Async loader (asyncpg)")
    parser.add_argument("--connections", "-c", type=int, default=50)
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    parser.add_argument("--mode", choices=["insert", "update"], default="insert")
    parser.add_argument("--no-sync", action="store_true", help="SET synchronous_commit = off")
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.rows, args.mode, args.no_sync))
