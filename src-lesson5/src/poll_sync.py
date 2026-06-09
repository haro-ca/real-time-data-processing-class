"""Naive polling-based "CDC" — the approach everyone tries first, and why it lies.

    SELECT * FROM orders WHERE updated_at > :last_sync

Looks airtight. It silently drifts because the source can't answer "what changed?"
honestly via a SELECT:
  * DELETEs leave no row to poll        -> deleted rows linger as GHOSTS in the copy
  * a write that forgets updated_at      -> that change is MISSED, the copy goes STALE
  * (only inserts/updates that bump updated_at are ever caught)

`--audit` runs an initial sync, injects a realistic mix of writes, polls again, and
then diffs the copy against the source to surface the drift — with NO error raised.

Usage:
    python src/poll_sync.py                  # a few naive poll ticks (looks fine)
    python src/poll_sync.py --audit          # inject writes, then reveal the drift
"""

import argparse

import duckdb

from config import DUCKDB_PATH, PG_CONN_STR

EPOCH = "1970-01-01"


def _attach() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG_CONN_STR}' AS pg (TYPE postgres)")
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders_polled (
            id BIGINT PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2),
            status VARCHAR, created_at TIMESTAMPTZ
        )
    """)
    return con


def poll(con: duckdb.DuckDBPyConnection, last_sync: str) -> tuple[int, str]:
    """One naive poll: upsert rows whose updated_at advanced since last_sync."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE chg AS
        SELECT id, customer_id, amount, status, created_at, updated_at
        FROM pg.orders WHERE updated_at > TIMESTAMP '{last_sync}'
    """)
    n = con.execute("SELECT count(*) FROM chg").fetchone()[0]
    if n:
        con.execute("DELETE FROM orders_polled WHERE id IN (SELECT id FROM chg)")
        con.execute("INSERT INTO orders_polled SELECT id, customer_id, amount, status, created_at FROM chg")
    hwm = con.execute("SELECT max(updated_at)::VARCHAR FROM chg").fetchone()[0]
    new_mark = hwm if hwm is not None else last_sync  # empty poll -> keep the mark
    return n, new_mark


def pg_exec(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.execute("CALL postgres_execute('pg', $$" + sql + "$$)")


def audit() -> None:
    con = _attach()
    con.execute("DELETE FROM orders_polled")

    print("Initial sync (everything so far)...")
    n, mark = poll(con, EPOCH)
    print(f"  synced {n:,} rows. high-water mark = {mark}\n")

    # Grab disjoint id ranges to mutate.
    ids = [r[0] for r in con.execute("SELECT id FROM pg.orders ORDER BY id LIMIT 40").fetchall()]
    stale_ids = ids[0:5]      # raw UPDATE, forgets updated_at  -> MISSED
    del_ids   = ids[5:17]     # DELETE                          -> GHOSTS
    bump_ids  = ids[17:23]    # UPDATE that bumps updated_at     -> caught

    print("Injecting a realistic write mix on the source:")
    pg_exec(con, "INSERT INTO orders (customer_id, amount, status) "
                 "SELECT 1, 9.99, 'pending' FROM generate_series(1, 8)")
    print("  +8 INSERTs (updated_at defaults to now)        -> should be caught")
    pg_exec(con, f"UPDATE orders SET status='shipped', updated_at=now() "
                 f"WHERE id IN ({','.join(map(str, bump_ids))})")
    print(f"  {len(bump_ids)} UPDATEs that bump updated_at              -> should be caught")
    pg_exec(con, f"UPDATE orders SET status='cancelled' "
                 f"WHERE id IN ({','.join(map(str, stale_ids))})")
    print(f"  {len(stale_ids)} raw UPDATEs that FORGET updated_at        -> will be missed")
    pg_exec(con, f"DELETE FROM orders WHERE id IN ({','.join(map(str, del_ids))})")
    print(f"  {len(del_ids)} DELETEs                                    -> invisible to polling\n")

    print("Polling again (WHERE updated_at > high-water mark)...")
    n, mark = poll(con, mark)
    print(f"  synced {n:,} rows (the inserts + bumped updates only)\n")

    # ── audit: diff the copy against the source ──
    src = con.execute("SELECT count(*) FROM pg.orders").fetchone()[0]
    cop = con.execute("SELECT count(*) FROM orders_polled").fetchone()[0]
    ghosts = con.execute("""
        SELECT count(*) FROM orders_polled p
        LEFT JOIN pg.orders s USING (id) WHERE s.id IS NULL
    """).fetchone()[0]
    stale = con.execute("""
        SELECT count(*) FROM orders_polled p
        JOIN pg.orders s USING (id) WHERE p.status <> s.status
    """).fetchone()[0]
    con.close()

    print("  AUDIT                              source        copy")
    print(f"  rows                            {src:>10,}  {cop:>10,}")
    print(f"  ghost rows (deleted, still in copy)              {ghosts:>6,}")
    print(f"  stale rows (status differs, missed UPDATE)       {stale:>6,}")
    print()
    drift = ghosts + stale
    if drift:
        print(f"  DRIFT: copy disagrees with source on {drift} rows. No error was raised.")
    else:
        print("  No drift detected (unexpected for this demo).")


def loop_demo(ticks: int) -> None:
    con = _attach()
    con.execute("DELETE FROM orders_polled")
    mark = EPOCH
    for t in range(1, ticks + 1):
        n, mark = poll(con, mark)
        print(f"  tick {t}: synced {n:,} rows since last poll (looks fine...)")
    con.close()
    print("\nLooks healthy — run with --audit to see what it silently misses.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Naive polling sync and why it drifts")
    p.add_argument("--audit", action="store_true", help="inject writes and reveal drift")
    p.add_argument("--ticks", type=int, default=3, help="poll ticks in plain (non-audit) mode")
    args = p.parse_args()
    if args.audit:
        audit()
    else:
        loop_demo(args.ticks)
