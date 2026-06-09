"""The disk-full time bomb: a replication slot with no consumer (slide 25).

Create a slot, NEVER read from it, then write to the source. Postgres cannot
recycle WAL the slot hasn't confirmed, so retained WAL climbs without bound until
it fills the disk and the source REFUSES WRITES — a full outage caused by a
forgotten slot.

This demo keeps the numbers small and safe (a few hundred MB at most) and DROPS
the slot at the end by default. Pass --keep to leave it (and watch watch_lag.py).

The production safety valve is `max_slot_wal_keep_size` (PG 13+): it drops a slot
that retains too much WAL to protect the database, at the cost of forcing the
consumer to re-snapshot. We deliberately leave it unset here so the growth shows.

Usage:
    python src/experiment_abandon_slot.py                 # 5 x 50k inserts, then drop
    python src/experiment_abandon_slot.py --iters 8 --batch 100000 --keep
"""

import argparse

import psycopg

from config import PG_DSN

ABANDONED = "abandoned_slot"

RETAINED_SQL = """
    SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)),
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    FROM pg_replication_slots WHERE slot_name = %s
"""


def run(iters: int, batch: int, keep: bool) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        exists = pg.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (ABANDONED,)
        ).fetchone()
        if not exists:
            pg.execute("SELECT pg_create_logical_replication_slot(%s, 'wal2json')", (ABANDONED,))
            print(f"created slot '{ABANDONED}' — and we will NEVER read from it.\n")

        print(f"{'iter':<6}{'inserted':>12}{'retained WAL':>16}")
        for i in range(1, iters + 1):
            pg.execute(
                """INSERT INTO orders (customer_id, amount, status)
                   SELECT (random()*49999)::int + 1,
                          round((5 + random()*495)::numeric, 2),
                          'pending'
                   FROM generate_series(1, %s)""",
                (batch,),
            )
            pretty, _ = pg.execute(RETAINED_SQL, (ABANDONED,)).fetchone()
            print(f"{i:<6}{i*batch:>12,}{pretty:>16}   <- nobody is consuming this")

        print("\nThe slot pins WAL the source can never free. Left alone under real")
        print("write load, 'retained' climbs until the disk fills and Postgres stops")
        print("accepting writes. The fix:")
        if keep:
            print(f"  SELECT pg_drop_replication_slot('{ABANDONED}');   (left running: --keep)")
        else:
            pg.execute("SELECT pg_drop_replication_slot(%s)", (ABANDONED,))
            print(f"  dropped '{ABANDONED}' — WAL can be recycled again.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Demonstrate WAL retention from an abandoned slot")
    p.add_argument("--iters", type=int, default=5)
    p.add_argument("--batch", type=int, default=50_000)
    p.add_argument("--keep", action="store_true", help="don't drop the slot at the end")
    args = p.parse_args()
    run(args.iters, args.batch, args.keep)
