"""Experiment: Sharded parallel COPY on CockroachDB.

Demonstrates that when each connection writes to a disjoint key range
(different shard/range), CockroachDB can exceed single-node Postgres throughput.

Uses the 3-node CockroachDB cluster from this lesson's docker-compose.yml
(brought up alongside Postgres via `docker compose up -d`).

Usage:
    uv run python experiment_sharded_copy.py [--ranges 8] [--conns 8] [--rows 100000]
"""

import argparse
import asyncio
import os
import random
import time

import asyncpg

# CockroachDB DSN — host/port default to localhost for native runs; the
# duckdb container overrides via CRDB_HOST=crdb-1 so it can reach the cluster
# on the docker network.
CRDB_HOST = os.environ.get("CRDB_HOST", "localhost")
CRDB_PORT = os.environ.get("CRDB_PORT", "26257")
DSN = f"postgresql://root@{CRDB_HOST}:{CRDB_PORT}/bench?sslmode=disable"


async def setup_ranges(n_ranges: int) -> None:
    """Create table and split into N ranges for even distribution."""
    conn = await asyncpg.connect(DSN)

    await conn.execute("DROP TABLE IF EXISTS orders_sharded")
    await conn.execute("""
        CREATE TABLE orders_sharded (
            id          INT8 PRIMARY KEY,
            customer_id INT4 NOT NULL,
            amount      DECIMAL(10, 2),
            created_at  TIMESTAMPTZ DEFAULT now()
        )
    """)

    # Split table at evenly spaced keys
    # Each range gets its own leaseholder → independent writes
    step = 10_000_000 // n_ranges
    for i in range(1, n_ranges):
        split_key = i * step
        await conn.execute(
            f"ALTER TABLE orders_sharded SPLIT AT VALUES ({split_key})"
        )

    # Scatter leases across nodes
    await conn.execute(
        "ALTER TABLE orders_sharded SCATTER"
    )

    await conn.close()
    print(f"  Created orders_sharded with {n_ranges} ranges (split + scattered)")


async def run(n_ranges: int, n_conns: int, total_rows: int, batch_size: int) -> None:
    print("Experiment: Sharded Parallel COPY (CockroachDB)")
    print(f"  ranges={n_ranges}  conns={n_conns}  rows={total_rows:,}  batch={batch_size}")
    print("-" * 60)

    await setup_ranges(n_ranges)

    pool = await asyncpg.create_pool(DSN, min_size=n_conns, max_size=n_conns)
    rows_per_worker = total_rows // n_conns
    step = 10_000_000 // n_ranges
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

    async def worker(worker_id: int, n_rows: int):
        """Each worker writes to a key range that maps to a specific shard."""
        key_start = worker_id * step
        async with pool.acquire() as conn:
            done = 0
            row_id = key_start
            while done < n_rows:
                chunk = min(batch_size, n_rows - done)
                records = [
                    (row_id + j, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                    for j in range(chunk)
                ]
                await conn.copy_records_to_table(
                    "orders_sharded",
                    records=records,
                    columns=["id", "customer_id", "amount"],
                )
                row_id += chunk
                done += chunk
                counter["done"] += chunk

    reporter_task = asyncio.create_task(reporter())
    tasks = [
        asyncio.create_task(worker(i, rows_per_worker))
        for i in range(n_conns)
    ]
    await asyncio.gather(*tasks)
    await reporter_task

    elapsed = time.monotonic() - t0
    tps = total_rows / elapsed
    print("-" * 60)
    print(f"Done. {total_rows:,} rows in {elapsed:.1f}s → {tps:,.0f} TPS")
    print(f"  ({n_conns} writers across {n_ranges} ranges — no cross-range contention)")
    print()
    print("  Note: on a laptop, 3 CRDB nodes share one machine's disk/CPU, so this")
    print("  almost certainly LOSES to single-node Postgres. The point of the demo")
    print("  is to show the *mechanism* — disjoint key ranges → independent writes")
    print("  with no coordination. On production hardware with separate nodes, this")
    print("  scales roughly linearly with shard count.")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sharded COPY on CockroachDB")
    parser.add_argument("--ranges", "-r", type=int, default=8,
                        help="Number of ranges to split table into (default: 8)")
    parser.add_argument("--conns", "-c", type=int, default=8,
                        help="Number of parallel connections (default: 8)")
    parser.add_argument("--rows", "-n", type=int, default=100_000,
                        help="Total rows to insert (default: 100,000)")
    parser.add_argument("--batch-size", "-b", type=int, default=1_000,
                        help="Rows per COPY batch (default: 1,000)")
    args = parser.parse_args()
    asyncio.run(run(args.ranges, args.conns, args.rows, args.batch_size))
