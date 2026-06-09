"""Wire up logical replication on the source: REPLICA IDENTITY, publication, slot.

This is the one-time CDC setup the slides build by hand (slide 14). Three pieces:

  1. REPLICA IDENTITY FULL  — so UPDATE/DELETE events carry the OLD row, not just
                              the primary key. (A materialized view only needs the
                              new values, but FULL keeps the demo honest for audits.)
  2. PUBLICATION            — declares which tables are in the stream (just orders).
  3. REPLICATION SLOT       — a durable, named cursor into the WAL using wal2json.
                              The returned consistent_point LSN is the boundary:
                              everything after it streams; everything before it
                              (the seeded million) does not — see snapshot.py.

Usage:
    python src/setup_cdc.py            # create slot if missing, print consistent_point
    python src/setup_cdc.py --reset    # drop + recreate the slot (fresh stream)
"""

import argparse
import sys

import psycopg

from config import PG_DSN, PLUGIN, PUBLICATION, SLOT


def slot_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (name,)
    ).fetchone()
    return row is not None


def setup(reset: bool) -> None:
    # autocommit: slot functions and ALTER/CREATE shouldn't sit in a long txn.
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        print("Wiring logical replication on public.orders ...")

        conn.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        print("  REPLICA IDENTITY FULL  set (UPDATE/DELETE carry old values)")

        conn.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION}")
        conn.execute(f"CREATE PUBLICATION {PUBLICATION} FOR TABLE orders")
        print(f"  PUBLICATION {PUBLICATION}  -> public.orders")

        if reset and slot_exists(conn, SLOT):
            conn.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
            print(f"  dropped existing slot {SLOT} (--reset)")

        if slot_exists(conn, SLOT):
            print(f"  slot {SLOT} already exists — leaving it (use --reset to recreate)")
        else:
            row = conn.execute(
                "SELECT lsn FROM pg_create_logical_replication_slot(%s, %s)",
                (SLOT, PLUGIN),
            ).fetchone()
            print(f"  SLOT {SLOT}  created with plugin={PLUGIN}")
            print(f"  consistent_point = {row[0]}   <- stream starts AFTER this LSN")

        # Show the boundary the snapshot must cover.
        lag = conn.execute(
            """
            SELECT pg_size_pretty(
                     pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
            FROM pg_replication_slots WHERE slot_name = %s
            """,
            (SLOT,),
        ).fetchone()
        print(f"\n  Source already holds ~{lag[0]} of WAL behind the slot.")
        print("  Those pre-slot rows are NOT in the stream -> run snapshot.py to backfill,")
        print("  then cdc_consumer.py to stream everything after.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="One-time CDC setup: identity + publication + slot")
    p.add_argument("--reset", action="store_true", help="drop and recreate the slot")
    args = p.parse_args()
    try:
        setup(args.reset)
    except psycopg.OperationalError as e:
        sys.exit(f"Cannot reach Postgres ({e}). Is `docker compose up -d` done?")
