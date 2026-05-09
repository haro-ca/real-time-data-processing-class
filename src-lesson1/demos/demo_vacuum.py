"""Demo — Show dead tuple accumulation and vacuum cleanup.

Disables autovacuum on the orders table, runs many UPDATEs to create
dead tuples, then shows before/after VACUUM stats.

Usage:
    python demos/demo_vacuum.py [--rows 50000] [--updates 100000]

Then in psql:
    SELECT relname, n_live_tup, n_dead_tup,
           n_dead_tup::float / NULLIF(n_live_tup + n_dead_tup, 0) AS dead_ratio,
           pg_size_pretty(pg_total_relation_size(relid)) AS total_size
    FROM pg_stat_user_tables WHERE relname = 'orders';
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"


async def run(seed_rows: int, update_count: int) -> None:
    pool = await asyncpg.create_pool(DSN, min_size=10, max_size=10)

    print("Demo — Dead tuple accumulation & VACUUM")
    print("-" * 60)

    # Step 1: Disable autovacuum
    async with pool.acquire() as conn:
        await conn.execute("ALTER TABLE orders SET (autovacuum_enabled = false)")
    print("  ✓ Autovacuum disabled on orders table\n")

    # Step 2: Seed rows if needed
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM orders")
    if count < seed_rows:
        print(f"  Seeding {seed_rows - count:,} rows...")
        async with pool.acquire() as conn:
            rows = [(random.randint(1, 10_000), round(random.uniform(1, 500), 2))
                    for _ in range(seed_rows - count)]
            await conn.executemany(
                "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)", rows)
        print(f"  ✓ Table has {seed_rows:,} rows\n")
    else:
        print(f"  ✓ Table already has {count:,} rows\n")

    # Step 3: Show before stats
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            "SELECT n_live_tup, n_dead_tup, "
            "pg_size_pretty(pg_total_relation_size('orders'::regclass)) AS size "
            "FROM pg_stat_user_tables WHERE relname = 'orders'")
    print(f"  BEFORE: live={stats['n_live_tup']:,}  dead={stats['n_dead_tup']:,}  size={stats['size']}")

    # Step 4: Run updates (creates dead tuples)
    print(f"\n  Running {update_count:,} UPDATEs (creating dead tuples)...")
    t0 = time.monotonic()

    async def worker(n: int):
        async with pool.acquire() as conn:
            for _ in range(n):
                row_id = random.randint(1, seed_rows)
                await conn.execute(
                    "UPDATE orders SET amount = amount + $1 WHERE id = $2",
                    round(random.uniform(0.01, 1.0), 2), row_id)

    per_worker = update_count // 10
    await asyncio.gather(*[worker(per_worker) for _ in range(10)])
    elapsed = time.monotonic() - t0
    print(f"  ✓ {update_count:,} UPDATEs in {elapsed:.1f}s\n")

    # Step 5: Show after stats (need to analyze first for fresh stats)
    async with pool.acquire() as conn:
        await conn.execute("ANALYZE orders")
        stats = await conn.fetchrow(
            "SELECT n_live_tup, n_dead_tup, "
            "pg_size_pretty(pg_total_relation_size('orders'::regclass)) AS size "
            "FROM pg_stat_user_tables WHERE relname = 'orders'")
    dead_ratio = stats['n_dead_tup'] / max(1, stats['n_live_tup'] + stats['n_dead_tup'])
    print(f"  AFTER:  live={stats['n_live_tup']:,}  dead={stats['n_dead_tup']:,}  size={stats['size']}")
    print(f"          dead ratio: {dead_ratio:.1%}")
    print(f"          ⚠ Table is bloated — dead tuples occupy space but are invisible to queries")

    # Step 6: Prompt for vacuum
    print("\n" + "-" * 60)
    print("  Now run VACUUM in psql to reclaim space:")
    print("    VACUUM VERBOSE orders;")
    print("\n  Then check stats again:")
    print("    SELECT n_live_tup, n_dead_tup,")
    print("           pg_size_pretty(pg_total_relation_size('orders'::regclass)) AS size")
    print("    FROM pg_stat_user_tables WHERE relname = 'orders';")
    print("\n  To re-enable autovacuum:")
    print("    ALTER TABLE orders SET (autovacuum_enabled = true);")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo — vacuum and dead tuples")
    parser.add_argument("--rows", "-n", type=int, default=50_000)
    parser.add_argument("--updates", "-u", type=int, default=100_000)
    args = parser.parse_args()
    asyncio.run(run(args.rows, args.updates))
