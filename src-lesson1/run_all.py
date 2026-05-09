"""Run all benchmark scenarios back-to-back and print a comparison table.

Usage:
    python run_all.py [--rows 50000]

Runs:
    1. Naive (1 conn, synchronous)
    2. Async (10 conns)
    3. Async (50 conns)
    4. Async (50 conns, sync_commit=off)
    5. COPY batch (batch_size=1000)
    6. Hot row (50 conns, 10s)
"""

import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"
INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"


async def reset():
    conn = await asyncpg.connect(DSN)
    await conn.execute("TRUNCATE orders RESTART IDENTITY")
    await conn.execute("SELECT pg_stat_statements_reset()")
    await conn.execute("SELECT pg_stat_reset()")
    await conn.close()


async def bench_naive(rows: int) -> float:
    conn = await asyncpg.connect(DSN)
    await conn.execute("SET synchronous_commit = on")
    t0 = time.monotonic()
    for _ in range(rows):
        await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
    elapsed = time.monotonic() - t0
    await conn.close()
    return rows / elapsed


async def bench_async(rows: int, connections: int, no_sync: bool = False) -> float:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    rows_per_worker = rows // connections
    remainder = rows % connections
    counter = {"done": 0}

    async def worker(n: int):
        conn = await pool.acquire()
        try:
            if no_sync:
                await conn.execute("SET synchronous_commit = off")
            for _ in range(n):
                await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                counter["done"] += 1
        finally:
            await pool.release(conn)

    t0 = time.monotonic()
    tasks = []
    for i in range(connections):
        n = rows_per_worker + (1 if i < remainder else 0)
        tasks.append(asyncio.create_task(worker(n)))
    await asyncio.gather(*tasks)
    elapsed = time.monotonic() - t0
    await pool.close()
    return rows / elapsed


async def bench_copy(rows: int, batch_size: int = 1000) -> float:
    conn = await asyncpg.connect(DSN)
    t0 = time.monotonic()
    for offset in range(0, rows, batch_size):
        batch = [(random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                 for _ in range(min(batch_size, rows - offset))]
        await conn.copy_records_to_table(
            "orders", records=batch, columns=["customer_id", "amount"])
    elapsed = time.monotonic() - t0
    await conn.close()
    return rows / elapsed


async def bench_parallel_copy(rows: int, connections: int = 4, batch_size: int = 1000) -> float:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    rows_per_worker = rows // connections
    remainder = rows % connections

    async def worker(n_rows: int):
        async with pool.acquire() as conn:
            done = 0
            while done < n_rows:
                chunk = min(batch_size, n_rows - done)
                batch = [(random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                         for _ in range(chunk)]
                await conn.copy_records_to_table(
                    "orders", records=batch, columns=["customer_id", "amount"])
                done += chunk

    t0 = time.monotonic()
    await asyncio.gather(*[
        worker(rows_per_worker + (1 if i < remainder else 0))
        for i in range(connections)
    ])
    elapsed = time.monotonic() - t0
    await pool.close()
    return rows / elapsed


async def bench_hotrow(connections: int, duration: int) -> float:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    # Ensure target row
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT id FROM orders WHERE id = 1")
        if row is None:
            await conn.execute(
                "INSERT INTO orders (id, customer_id, amount) "
                "OVERRIDING SYSTEM VALUE VALUES (1, 1, 10.00)")

    counter = {"done": 0}
    running = True

    async def worker():
        nonlocal running
        async with pool.acquire() as conn:
            while running:
                await conn.execute(
                    "UPDATE orders SET amount = amount + $1 WHERE id = 1",
                    round(random.uniform(0.01, 1.0), 2))
                counter["done"] += 1

    tasks = [asyncio.create_task(worker()) for _ in range(connections)]
    await asyncio.sleep(duration)
    running = False
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    return counter["done"] / duration


async def main(rows: int) -> None:
    results = []

    scenarios = [
        ("Naive (1 conn, sync)", lambda: bench_naive(rows)),
        ("Async (10 conns)", lambda: bench_async(rows, 10)),
        ("Async (50 conns)", lambda: bench_async(rows, 50)),
        ("Async (50, sync=off)", lambda: bench_async(rows, 50, no_sync=True)),
        ("COPY (batch=1000)", lambda: bench_copy(rows)),
        ("COPY (4 parallel)", lambda: bench_parallel_copy(rows, connections=4)),
        ("Hot row (50 conns, 10s)", lambda: bench_hotrow(50, 10)),
    ]

    print(f"Running all benchmarks with {rows:,} rows each...")
    print("=" * 60)

    for name, fn in scenarios:
        await reset()
        print(f"  Running: {name}...", end=" ", flush=True)
        tps = await fn()
        results.append((name, tps))
        print(f"{tps:,.0f} TPS")

    # Print final table
    print("\n" + "=" * 60)
    print(f"{'Scenario':<28} {'TPS':>10} {'vs Naive':>10}")
    print("-" * 60)
    baseline = results[0][1]
    for name, tps in results:
        ratio = tps / baseline
        print(f"  {name:<26} {tps:>9,.0f}   {ratio:>7.1f}×")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run all benchmarks and compare")
    parser.add_argument("--rows", "-n", type=int, default=50_000)
    args = parser.parse_args()
    asyncio.run(main(args.rows))
