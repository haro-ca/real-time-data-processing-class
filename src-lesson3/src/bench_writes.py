"""Write benchmark: Postgres vs CockroachDB (RF=3 and RF=1/3-shard).
Scenarios: naive INSERTs, COPY, hot-row contention, cross-range 2PC.
Usage:
    uv run python src/bench_writes.py
    ./bench python src/bench_writes.py
"""
import asyncio, os, random, time
import asyncpg

PG_DSN   = f"postgresql://bench:bench@{os.environ.get('PG_HOST','localhost')}:{os.environ.get('PG_PORT','5432')}/bench"
CRDB_DSN = f"postgresql://root@{os.environ.get('CRDB_HOST','localhost')}:{os.environ.get('CRDB_PORT','26257')}/bench?sslmode=disable"

CONNS = 50; INSERT_ROWS = 50_000; COPY_ROWS = 200_000; BATCH = 5_000; HOT_SECS = 10
SPLITS      = [3_333_333, 6_666_666]
SHARD_BASES = [200_000,   3_533_333, 6_866_666]
HOT_IDS     = [1,         3_333_334, 6_666_668]


def pct(lats, p):
    if not lats: return 0.0
    s = sorted(lats); k = (len(s)-1)*(p/100); lo = int(k); hi = min(lo+1,len(s)-1)
    return s[lo] + (k-lo)*(s[hi]-s[lo])

def result(n, elapsed, lats):
    return {"tps": n/elapsed if elapsed else 0, "p50": pct(lats,50)*1000, "p95": pct(lats,95)*1000}


# ── Setup ────────────────────────────────────────────────────────────────────

async def pg_setup():
    c = await asyncpg.connect(PG_DSN)
    await c.execute("DROP TABLE IF EXISTS orders_bench")
    await c.execute("CREATE TABLE orders_bench (id BIGINT PRIMARY KEY, customer_id INT NOT NULL, amount NUMERIC(10,2))")
    await c.execute("INSERT INTO orders_bench VALUES (1,1,100.00)")
    await c.close()


async def crdb_set_rf(rf):
    c = await asyncpg.connect(CRDB_DSN)
    await c.execute(f"ALTER DATABASE bench CONFIGURE ZONE USING num_replicas = {rf}")
    await c.close()


async def crdb_setup(n_shards=1):
    c = await asyncpg.connect(CRDB_DSN)
    await c.execute("DROP TABLE IF EXISTS orders_bench")
    await c.execute("CREATE TABLE orders_bench (id INT8 PRIMARY KEY, customer_id INT4 NOT NULL, amount DECIMAL(10,2))")
    if n_shards > 1:
        for sp in SPLITS:
            await c.execute(f"ALTER TABLE orders_bench SPLIT AT VALUES ({sp})")
        await c.execute("ALTER TABLE orders_bench SCATTER")
    else:
        await c.execute("ALTER TABLE orders_bench SPLIT AT VALUES (4000000)")
    for hid in HOT_IDS:
        await c.execute(f"UPSERT INTO orders_bench VALUES ({hid},1,100.00)")
    await c.execute(f"UPSERT INTO orders_bench VALUES (8000000,1,100.00)")
    await c.close()
    if n_shards > 1:
        await asyncio.sleep(3)


# ── Benchmarks ───────────────────────────────────────────────────────────────

async def bench_inserts(dsn, conns, rows, shard_aware=False):
    pool = await asyncpg.create_pool(dsn, min_size=conns, max_size=conns)
    rpc = rows // conns; lats = []; t0 = time.monotonic()

    async def worker(wid):
        base = (SHARD_BASES[wid % 3] + (wid//3)*rpc) if shard_aware else wid*1_000_000+100_000
        async with pool.acquire() as conn:
            for i in range(rpc):
                for _ in range(20):
                    try:
                        t_op = time.monotonic()
                        await conn.execute("INSERT INTO orders_bench VALUES ($1,$2,$3)",
                            base+i, random.randint(1,10_000), round(random.uniform(1,500),2))
                        lats.append(time.monotonic()-t_op); break
                    except (asyncpg.SerializationError, asyncpg.UniqueViolationError):
                        base += 50_000_000

    await asyncio.gather(*[worker(i) for i in range(conns)])
    elapsed = time.monotonic()-t0; await pool.close()
    return result(rows, elapsed, lats)


async def bench_copy(dsn, rows, batch, n_writers=1):
    pool = await asyncpg.create_pool(dsn, min_size=n_writers, max_size=n_writers)
    rpc = rows // n_writers; t0 = time.monotonic()

    async def writer(wid):
        base = SHARD_BASES[wid] + 2_000_000 if n_writers > 1 else 300_000_000
        async with pool.acquire() as conn:
            done = 0
            while done < rpc:
                chunk = min(batch, rpc-done)
                recs = [(base+done+j, random.randint(1,10_000), round(random.uniform(1,500),2)) for j in range(chunk)]
                await conn.copy_records_to_table("orders_bench", records=recs, columns=["id","customer_id","amount"])
                done += chunk

    await asyncio.gather(*[writer(i) for i in range(n_writers)])
    elapsed = time.monotonic()-t0; await pool.close()
    return result(rows, elapsed, [])


async def bench_hotrow(dsn, conns, secs, n_hot=1, cross_range=False):
    pool = await asyncpg.create_pool(dsn, min_size=conns, max_size=conns)
    done = [0]; lats = []; running = True

    async def worker(wid):
        hid = HOT_IDS[wid % n_hot]
        async with pool.acquire() as conn:
            while running:
                try:
                    t_op = time.monotonic()
                    if cross_range:
                        async with conn.transaction():
                            await conn.execute("UPDATE orders_bench SET amount=amount+$1 WHERE id=1",   round(random.uniform(.01,1),2))
                            await conn.execute("UPDATE orders_bench SET amount=amount+$1 WHERE id=8000000", round(random.uniform(.01,1),2))
                    else:
                        await conn.execute("UPDATE orders_bench SET amount=amount+$1 WHERE id=$2",
                            round(random.uniform(.01,1),2), hid)
                    lats.append(time.monotonic()-t_op); done[0] += 1
                except asyncpg.SerializationError:
                    pass

    tasks = [asyncio.create_task(worker(i)) for i in range(conns)]
    await asyncio.sleep(secs); running = False
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    return result(done[0], secs, lats)


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    W = 72
    print("="*W)
    print("  Lesson 3 — Write Benchmark: Postgres vs CockroachDB")
    print("="*W)

    print("\n  [Postgres 2CPU/4GB]")
    await pg_setup()
    pg_ins  = await bench_inserts(PG_DSN, CONNS, INSERT_ROWS)
    pg_copy = await bench_copy(PG_DSN, COPY_ROWS, BATCH)
    pg_hot  = await bench_hotrow(PG_DSN, CONNS, HOT_SECS)
    for lbl, r in [("INSERT 50c", pg_ins), ("COPY 1c", pg_copy), ("Hot row 50c", pg_hot)]:
        p50 = f"p50={r['p50']:.1f}ms" if r['p50'] else ""
        print(f"    {lbl:<18} {r['tps']:>9,.0f} TPS  {p50}")

    print("\n  [CRDB RF=3, Raft on]")
    await crdb_set_rf(3); await crdb_setup(n_shards=1)
    cd3_ins   = await bench_inserts(CRDB_DSN, CONNS, INSERT_ROWS)
    cd3_copy  = await bench_copy(CRDB_DSN, COPY_ROWS, BATCH)
    cd3_hot_l = await bench_hotrow(CRDB_DSN, CONNS, HOT_SECS)
    cd3_hot_x = await bench_hotrow(CRDB_DSN, CONNS, HOT_SECS, cross_range=True)
    for lbl, r in [("INSERT 50c",cd3_ins),("COPY 1c",cd3_copy),("Hot local",cd3_hot_l),("Hot cross-range",cd3_hot_x)]:
        p50 = f"p50={r['p50']:.1f}ms" if r['p50'] else ""
        print(f"    {lbl:<18} {r['tps']:>9,.0f} TPS  {p50}")

    print("\n  [CRDB RF=1, 3 shards, 1 per node]")
    await crdb_set_rf(1); await crdb_setup(n_shards=3)
    cd1_ins  = await bench_inserts(CRDB_DSN, CONNS, INSERT_ROWS, shard_aware=True)
    cd1_copy = await bench_copy(CRDB_DSN, COPY_ROWS, BATCH, n_writers=3)
    cd1_hot  = await bench_hotrow(CRDB_DSN, CONNS, HOT_SECS, n_hot=3)
    for lbl, r in [("INSERT 50c",cd1_ins),("COPY 3c",cd1_copy),("Hot 3 rows",cd1_hot)]:
        p50 = f"p50={r['p50']:.1f}ms" if r['p50'] else ""
        print(f"    {lbl:<18} {r['tps']:>9,.0f} TPS  {p50}")

    # ── Summary ──────────────────────────────────────────────────
    print("\n"+"="*W)
    print(f"  {'Scenario':<42} {'TPS':>9}  {'p50ms':>7}  {'p95ms':>7}")
    print("  "+"-"*(W-2))
    rows_data = [
        ("PG  · INSERT 50 conns",              pg_ins),
        ("PG  · COPY (1 conn, batch=5k)",       pg_copy),
        ("PG  · Hot row (50 conns)",            pg_hot),
        ("CRDB RF=3 · INSERT 50 conns",         cd3_ins),
        ("CRDB RF=3 · COPY (1 conn)",           cd3_copy),
        ("CRDB RF=3 · Hot row (local)",         cd3_hot_l),
        ("CRDB RF=3 · Hot row (cross-range)",   cd3_hot_x),
        ("CRDB RF=1/3sh · INSERT 50 conns",     cd1_ins),
        ("CRDB RF=1/3sh · COPY (3 conns)",      cd1_copy),
        ("CRDB RF=1/3sh · Hot row (3 rows)",    cd1_hot),
    ]
    for lbl, r in rows_data:
        p50 = f"{r['p50']:.1f}" if r['p50'] else "—"
        p95 = f"{r['p95']:.1f}" if r['p95'] else "—"
        print(f"  {lbl:<42} {r['tps']:>9,.0f}  {p50:>7}  {p95:>7}")
    print("="*W)


if __name__ == "__main__":
    asyncio.run(main())
