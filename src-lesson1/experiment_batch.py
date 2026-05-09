"""Experiment A — Batching: use executemany / COPY to reduce round-trips and WAL flushes.

Usage:
    python experiment_batch.py [--rows 100000] [--batch-size 1000] [--method executemany]

Methods:
    executemany  — asyncpg executemany (default)
    copy         — asyncpg copy_records_to_table (fastest)
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


async def run_executemany(pool: asyncpg.Pool, total: int, batch_size: int) -> tuple[int, float]:
    done = 0
    t0 = time.monotonic()

    while done < total:
        chunk = min(batch_size, total - done)
        batch = make_batch(chunk)
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)",
                batch,
            )
        done += chunk
        elapsed = time.monotonic() - t0
        tps = done / elapsed
        print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {done:,}")

    return done, time.monotonic() - t0


async def run_copy(pool: asyncpg.Pool, total: int, batch_size: int) -> tuple[int, float]:
    done = 0
    t0 = time.monotonic()

    while done < total:
        chunk = min(batch_size, total - done)
        batch = make_batch(chunk)
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "orders",
                records=batch,
                columns=["customer_id", "amount"],
            )
        done += chunk
        elapsed = time.monotonic() - t0
        tps = done / elapsed
        print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {done:,}")

    return done, time.monotonic() - t0


async def run(total_rows: int, batch_size: int, method: str) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=5, max_size=5)

    print(f"Experiment A — Batching: {total_rows:,} rows, batch_size={batch_size}, method={method}")
    print("-" * 60)

    if method == "executemany":
        done, elapsed = await run_executemany(pool, total_rows, batch_size)
    else:
        done, elapsed = await run_copy(pool, total_rows, batch_size)

    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} rows in {elapsed:.1f}s → {tps:,.0f} TPS")
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment A — Batching")
    parser.add_argument("--rows", "-n", type=int, default=100_000)
    parser.add_argument("--batch-size", "-b", type=int, default=1_000)
    parser.add_argument("--method", choices=["executemany", "copy"], default="executemany")
    args = parser.parse_args()
    asyncio.run(run(args.rows, args.batch_size, args.method))
