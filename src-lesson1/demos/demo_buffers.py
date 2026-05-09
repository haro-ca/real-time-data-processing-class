"""Slide 9 demo — Load data into the buffer pool, then inspect pg_buffercache.

Inserts rows so the orders table occupies multiple 8KB pages in shared_buffers.
Then prints the query for you to run in psql.

Usage:
    python demos/demo_buffers.py [--rows 50000]

Then in psql:
    SELECT c.relname, count(*) AS buffers,
           pg_size_pretty(count(*) * 8192) AS size
    FROM pg_buffercache b
    JOIN pg_class c ON b.relfilenode = c.relfilenode
    WHERE b.reldatabase = (SELECT oid FROM pg_database WHERE datname = 'bench')
    GROUP BY c.relname ORDER BY 2 DESC LIMIT 10;
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"

INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"


async def run(total_rows: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=10, max_size=10)

    print(f"Loading {total_rows:,} rows to populate the buffer pool...")
    t0 = time.monotonic()

    sem = asyncio.Semaphore(40)

    async def insert_one():
        async with sem:
            async with pool.acquire() as conn:
                await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))

    await asyncio.gather(*[insert_one() for _ in range(total_rows)])
    elapsed = time.monotonic() - t0

    print(f"  ✓ {total_rows:,} rows loaded in {elapsed:.1f}s\n")
    print("Now run a sequential scan to pull pages into shared_buffers:")
    print("  SELECT count(*) FROM orders;\n")
    print("Then inspect the buffer pool:")
    print("  SELECT c.relname, count(*) AS buffers,")
    print("         pg_size_pretty(count(*) * 8192) AS size")
    print("  FROM pg_buffercache b")
    print("  JOIN pg_class c ON b.relfilenode = c.relfilenode")
    print("  WHERE b.reldatabase = (SELECT oid FROM pg_database WHERE datname = 'bench')")
    print("  GROUP BY c.relname ORDER BY 2 DESC LIMIT 10;")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slide 9 demo — buffer pool")
    parser.add_argument("--rows", "-n", type=int, default=50_000)
    args = parser.parse_args()
    asyncio.run(run(args.rows))
