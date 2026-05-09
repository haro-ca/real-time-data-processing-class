"""Demo — Provoke an OOM kill on the Postgres container.

Opens connections rapidly and has each one allocate memory
by loading a large result set. Container limit is 4GB.

Usage:
    python demos/demo_oom.py [--connections 500]
"""

import argparse
import asyncio

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"

# Each connection will hold a large result set in backend memory
BLOAT_SQL = """
    SELECT string_agg(md5(random()::text), '') 
    FROM generate_series(1, 50000)
"""


async def run(target: int) -> None:
    print(f"OOM demo — opening {target} connections, then bloating ALL at once")
    print("Container limit: 4GB RAM. This WILL kill the container.")
    print("-" * 60)

    # Phase 1: open all connections
    conns: list[asyncpg.Connection] = []
    print(f"\n  Phase 1: Opening {target} connections...")
    for i in range(target):
        try:
            conn = await asyncpg.connect(DSN)
            conns.append(conn)
            if (i + 1) % 100 == 0:
                print(f"    {len(conns)} connections open")
        except Exception as e:
            print(f"    ✗ Failed at {len(conns)} connections: {e}")
            break

    print(f"  ✓ {len(conns)} connections open\n")

    # Phase 2: fire bloat query on ALL connections simultaneously
    # Each backend allocates ~50MB. All held at the same time = OOM.
    print(f"  Phase 2: Firing bloat query on all {len(conns)} connections simultaneously...")
    print(f"           Target memory: {len(conns)} × ~50MB = ~{len(conns) * 50 // 1024}GB")
    print(f"           Container limit: 4GB")
    print()

    async def bloat(i: int, conn: asyncpg.Connection):
        try:
            await conn.fetchval(BLOAT_SQL)
        except Exception as e:
            print(f"    ✗ Connection {i} died: {e}")
            raise

    try:
        await asyncio.gather(*[bloat(i, c) for i, c in enumerate(conns)])
        print("  ✓ All queries returned. Container survived (unexpected).")
    except Exception as e:
        print(f"\n  ✗ Cascade failure: {e}")
        print("  Container likely OOM-killed. Check: docker compose ps")

    # Cleanup if we somehow survive
    for c in conns:
        try:
            await c.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OOM kill demo")
    parser.add_argument("--connections", "-c", type=int, default=500)
    args = parser.parse_args()
    asyncio.run(run(args.connections))
