"""Experiment C — Parallel COPY: N concurrent COPY writers, independent rows.

Demonstrates that parallel COPY on disjoint data scales nearly linearly —
once the per-row fsync overhead is gone, adding connections still helps.

Compare:
    COPY (1 conn)   →  experiment_batch.py --method copy
    COPY (N conns)  →  this script -c N

Usage:
    python experiment_parallel_copy.py [-c 4] [--rows 400000] [--batch-size 1000]
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"


def make_batch(size: int) -> list[tuple]:
    return [
        (random.randint(1, 10_000), round(random.uniform(1, 500), 2))
        for _ in range(size)
    ]


async def run(connections: int, total_rows: int, batch_size: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)

    print(f"Experiment C — Parallel COPY")
    print(f"  workers={connections}  rows={total_rows:,}  batch_size={batch_size}")
    print("-" * 60)

    rows_per_worker = total_rows // connections
    remainder = total_rows % connections
    counter = {"done": 0}
    t0 = time.monotonic()

    async def reporter():
        while True:
            await asyncio.sleep(1.0)
            elapsed = time.monotonic() - t0
            current = counter["done"]
            if current > 0:
                tps = current / elapsed
                print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {current:,}")
            if current >= total_rows:
                break

    async def worker(n_rows: int):
        async with pool.acquire() as conn:
            done = 0
            while done < n_rows:
                chunk = min(batch_size, n_rows - done)
                await conn.copy_records_to_table(
                    "orders",
                    records=make_batch(chunk),
                    columns=["customer_id", "amount"],
                )
                done += chunk
                counter["done"] += chunk

    reporter_task = asyncio.create_task(reporter())
    tasks = [
        asyncio.create_task(
            worker(rows_per_worker + (1 if i < remainder else 0))
        )
        for i in range(connections)
    ]
    await asyncio.gather(*tasks)
    await reporter_task

    elapsed = time.monotonic() - t0
    tps = total_rows / elapsed
    print("-" * 60)
    print(f"Done. {total_rows:,} rows in {elapsed:.1f}s → {tps:,.0f} TPS")
    print(f"  ({connections} parallel COPY writers, batch_size={batch_size})")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment C — Parallel COPY")
    parser.add_argument("--connections", "-c", type=int, default=4,
                        help="Number of parallel COPY workers (default: 4)")
    parser.add_argument("--rows", "-n", type=int, default=400_000,
                        help="Total rows to insert (default: 400,000)")
    parser.add_argument("--batch-size", "-b", type=int, default=1_000,
                        help="Rows per COPY call per worker (default: 1,000)")
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.rows, args.batch_size))
