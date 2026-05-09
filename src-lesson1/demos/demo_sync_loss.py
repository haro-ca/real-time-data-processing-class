"""Demo — Show data loss with synchronous_commit = off.

Inserts rows as fast as possible with sync commit disabled,
then crashes the container mid-flight. On restart, the gap
between what the client saw vs what survived = lost data.

Usage:
    python demos/demo_sync_loss.py [--seconds 5]

Requires:
    - lesson1-postgres container running via docker compose
    - docker CLI accessible
"""

import argparse
import asyncio
import subprocess
import sys
import time

import asyncpg

DSN = "postgresql://bench:bench@localhost:5432/bench"
CONTAINER = "lesson1-postgres"


async def run(duration: float) -> None:
    print("Demo — synchronous_commit = off data loss")
    print("=" * 60)

    # ── Step 1: Connect and enable sync_commit = off ─────────
    conn = await asyncpg.connect(DSN)
    await conn.execute("SET synchronous_commit = off")

    # Use a dedicated table so we don't pollute orders
    await conn.execute("""
        DROP TABLE IF EXISTS sync_loss_demo;
        CREATE TABLE sync_loss_demo (
            seq  BIGINT NOT NULL,
            ts   TIMESTAMPTZ DEFAULT now()
        )
    """)
    print("  ✓ Table sync_loss_demo created")
    print("  ✓ synchronous_commit = off\n")

    # ── Step 2: Insert rows in a tight loop ──────────────────
    print(f"  Inserting rows for ~{duration:.0f}s with sync commit OFF...")
    print("  (each INSERT gets ACK before WAL is fsynced)\n")

    client_acked = 0
    t0 = time.monotonic()
    last_report = t0

    try:
        while time.monotonic() - t0 < duration:
            client_acked += 1
            await conn.execute(
                "INSERT INTO sync_loss_demo (seq) VALUES ($1)", client_acked
            )
            # Report progress every second
            now = time.monotonic()
            if now - last_report >= 1.0:
                elapsed = now - t0
                tps = client_acked / elapsed
                print(f"    [{elapsed:5.1f}s]  {client_acked:>8,} rows inserted  ({tps:,.0f} TPS)")
                last_report = now
    except (asyncpg.ConnectionDoesNotExistError, asyncpg.InterfaceError, OSError):
        # Container died while we were inserting — expected
        pass

    elapsed = time.monotonic() - t0
    print(f"\n  Client saw {client_acked:,} acknowledged inserts in {elapsed:.1f}s")

    # ── Step 3: Kill the container (simulate crash) ──────────
    print(f"\n  💥 Killing container '{CONTAINER}' with SIGKILL (no graceful shutdown)...")
    result = subprocess.run(
        ["docker", "kill", CONTAINER],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ⚠ docker kill failed: {result.stderr.strip()}")
        print("  Container may have already died from insertion pressure.")
    else:
        print("  ✓ Container killed")

    # ── Step 4: Restart and wait for healthy ─────────────────
    print("\n  Restarting container...")
    subprocess.run(
        ["docker", "start", CONTAINER],
        capture_output=True, text=True, check=True
    )

    # Wait for Postgres to be ready
    print("  Waiting for Postgres to recover (WAL replay)...", end="", flush=True)
    for attempt in range(30):
        try:
            check_conn = await asyncpg.connect(DSN)
            await check_conn.close()
            print(" ready!")
            break
        except Exception:
            print(".", end="", flush=True)
            await asyncio.sleep(1)
    else:
        print("\n  ✗ Postgres didn't come back in 30s. Check docker logs.")
        sys.exit(1)

    # ── Step 5: Count what survived ──────────────────────────
    conn2 = await asyncpg.connect(DSN)
    survived = await conn2.fetchval("SELECT COALESCE(MAX(seq), 0) FROM sync_loss_demo")
    await conn2.close()

    lost = client_acked - survived

    print("\n" + "=" * 60)
    print("  Results:")
    print(f"    Client acknowledged:  {client_acked:>10,} rows")
    print(f"    Survived on disk:     {survived:>10,} rows")
    print("    ─────────────────────────────────")
    print(f"    LOST:                 {lost:>10,} rows")

    if client_acked > 0:
        loss_pct = lost / client_acked * 100
        print(f"    Loss ratio:           {loss_pct:>9.2f}%")

    if lost > 0:
        print(f"\n  ⚠ These {lost:,} rows were acknowledged to the client")
        print("    but never made it to durable storage.")
        print("    This is the cost of synchronous_commit = off.")
    else:
        print("\n  ✓ No data lost this time — the WAL writer flushed in time.")
        print("    Run again or reduce --seconds to catch the window.")

    print("\n  Cleanup: DROP TABLE sync_loss_demo;")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Demo — data loss with synchronous_commit = off"
    )
    parser.add_argument(
        "--seconds", "-s", type=float, default=5,
        help="How long to insert before crashing (default: 5)"
    )
    args = parser.parse_args()
    asyncio.run(run(args.seconds))
