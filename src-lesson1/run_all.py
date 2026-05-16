"""Run all benchmark scenarios back-to-back and print a comparison table.

Usage:
    python run_all.py [--rows 50000]

Runs:
    1. Naive (1 conn, synchronous)
    2. Async (50 conns)
    3. Naive (1 conn, sync_commit=off)
    4. Async (50 conns, sync_commit=off)
    5. COPY batch (batch_size=1000)
    6. COPY (4 parallel)
    7. Hot row (50 conns, 10s)
"""

import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"
INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"


def pct(latencies: list[float], p: float) -> float:
    if not latencies:
        return 0.0
    latencies.sort()
    k = (len(latencies) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(latencies) else f
    return latencies[f] + (k - f) * (latencies[c] - latencies[f])


def make_result(tps: float, latencies: list[float]) -> dict:
    return {
        "tps": tps,
        "p50": pct(latencies, 50) * 1000,
        "p95": pct(latencies, 95) * 1000,
        "p99": pct(latencies, 99) * 1000,
    }


async def reset():
    conn = await asyncpg.connect(DSN)
    await conn.execute("TRUNCATE orders RESTART IDENTITY")
    await conn.execute("SELECT pg_stat_statements_reset()")
    await conn.execute("SELECT pg_stat_reset()")
    await conn.close()


async def bench_naive(rows: int, no_sync: bool = False) -> dict:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f"SET synchronous_commit = {'off' if no_sync else 'on'}")
    lats = []
    t0 = time.monotonic()
    for _ in range(rows):
        t_op = time.monotonic()
        await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
        lats.append(time.monotonic() - t_op)
    elapsed = time.monotonic() - t0
    await conn.close()
    return make_result(rows / elapsed, lats)


async def bench_async(rows: int, connections: int, no_sync: bool = False) -> dict:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    rows_per_worker = rows // connections
    remainder = rows % connections
    lats: list[float] = []

    async def worker(n: int):
        conn = await pool.acquire()
        try:
            if no_sync:
                await conn.execute("SET synchronous_commit = off")
            for _ in range(n):
                t_op = time.monotonic()
                await conn.execute(INSERT_SQL, random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                lats.append(time.monotonic() - t_op)
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
    return make_result(rows / elapsed, lats)


async def bench_copy(rows: int, batch_size: int = 1000) -> dict:
    conn = await asyncpg.connect(DSN)
    lats = []
    t0 = time.monotonic()
    batch_num = 0
    for offset in range(0, rows, batch_size):
        chunk = min(batch_size, rows - offset)
        batch = [(random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                 for _ in range(chunk)]
        sample = (batch_num % 10 == 0)
        if sample:
            t_op = time.monotonic()
        await conn.copy_records_to_table(
            "orders", records=batch, columns=["customer_id", "amount"])
        if sample:
            lats.append((time.monotonic() - t_op) / chunk)
        batch_num += 1
    elapsed = time.monotonic() - t0
    await conn.close()
    return make_result(rows / elapsed, lats)


async def bench_parallel_copy(rows: int, connections: int = 4, batch_size: int = 1000) -> dict:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    rows_per_worker = rows // connections
    remainder = rows % connections
    lats: list[float] = []

    async def worker(n_rows: int):
        async with pool.acquire() as conn:
            done = 0
            batch_num = 0
            while done < n_rows:
                chunk = min(batch_size, n_rows - done)
                batch = [(random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                         for _ in range(chunk)]
                sample = (batch_num % 10 == 0)
                if sample:
                    t_op = time.monotonic()
                await conn.copy_records_to_table(
                    "orders", records=batch, columns=["customer_id", "amount"])
                if sample:
                    lats.append((time.monotonic() - t_op) / chunk)
                done += chunk
                batch_num += 1

    t0 = time.monotonic()
    await asyncio.gather(*[
        worker(rows_per_worker + (1 if i < remainder else 0))
        for i in range(connections)
    ])
    elapsed = time.monotonic() - t0
    await pool.close()
    return make_result(rows / elapsed, lats)


async def bench_hotrow(connections: int, duration: int) -> dict:
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    # Ensure target row
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT id FROM orders WHERE id = 1")
        if row is None:
            await conn.execute(
                "INSERT INTO orders (id, customer_id, amount) "
                "OVERRIDING SYSTEM VALUE VALUES (1, 1, 10.00)")

    counter = {"done": 0}
    lats: list[float] = []
    running = True

    async def worker():
        nonlocal running
        async with pool.acquire() as conn:
            while running:
                t_op = time.monotonic()
                await conn.execute(
                    "UPDATE orders SET amount = amount + $1 WHERE id = 1",
                    round(random.uniform(0.01, 1.0), 2))
                lats.append(time.monotonic() - t_op)
                counter["done"] += 1

    tasks = [asyncio.create_task(worker()) for _ in range(connections)]
    await asyncio.sleep(duration)
    running = False
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    return make_result(counter["done"] / duration, lats)


async def main(rows: int) -> None:
    results = []

    scenarios = [
        ("Naive (1 conn, sync)", lambda: bench_naive(rows)),
        ("Async (50 conns)", lambda: bench_async(rows, 50)),
        ("Naive (1 conn, sync=off)", lambda: bench_naive(rows, no_sync=True)),
        ("Async (50, sync=off)", lambda: bench_async(rows, 50, no_sync=True)),
        ("COPY (batch=1000)", lambda: bench_copy(rows)),
        ("COPY (4 parallel)", lambda: bench_parallel_copy(rows, connections=4)),
        ("Hot row (50 conns, 10s)", lambda: bench_hotrow(50, 10)),
    ]

    print(f"Running all benchmarks with {rows:,} rows each...")
    print("=" * 85)

    for name, fn in scenarios:
        await reset()
        print(f"  Running: {name}...", end=" ", flush=True)
        r = await fn()
        results.append((name, r))
        print(f"{r['tps']:,.0f} TPS  (p50={r['p50']:.2f}ms  p95={r['p95']:.2f}ms  p99={r['p99']:.2f}ms)")

    # Print final table
    print("\n" + "=" * 85)
    print(f"{'Scenario':<28} {'TPS':>8} {'vs Naive':>9} {'p50ms':>8} {'p95ms':>8} {'p99ms':>8}")
    print("-" * 85)
    baseline = results[0][1]["tps"]
    for name, r in results:
        ratio = r["tps"] / baseline
        print(f"  {name:<26} {r['tps']:>7,.0f}   {ratio:>7.1f}×"
              f" {r['p50']:>8.2f} {r['p95']:>8.2f} {r['p99']:>8.2f}")
    print("=" * 85)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run all benchmarks and compare")
    parser.add_argument("--rows", "-n", type=int, default=50_000)
    args = parser.parse_args()
    asyncio.run(main(args.rows))
