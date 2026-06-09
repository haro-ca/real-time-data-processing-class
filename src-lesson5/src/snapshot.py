"""Initial snapshot: backfill the rows that existed BEFORE the slot.

The slot only carries changes after its consistent_point, so the seeded million
rows are invisible to the stream (slide 21, "the missing million"). This does the
one-time bulk copy that closes the gap, then the consumer streams everything after.

Consistency: a single SELECT already sees one MVCC snapshot, so the bulk copy is
internally consistent. The honest caveat for a by-hand demo: the slot's LSN is
slightly *before* this snapshot, so the consumer may re-deliver a handful of
changes that the snapshot already contains. That's fine — the apply is idempotent
(delete+insert), so re-applying them is a no-op. (Debezium gets exact no-overlap by
exporting the slot's snapshot over the replication protocol; we trade that for
plain SQL + idempotency.)

Usage:
    python src/snapshot.py              # truncate mirror, bulk-copy orders
    python src/snapshot.py --no-truncate
"""

import argparse
import time

from config import PG_CONN_STR, connect_target


def snapshot(truncate: bool) -> None:
    con = connect_target()
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG_CONN_STR}' AS pg (TYPE postgres)")

    if truncate:
        con.execute("DELETE FROM orders")
        print("  mirror cleared (clean baseline)")

    t0 = time.monotonic()
    con.execute("""
        INSERT INTO orders (id, customer_id, amount, status, created_at, _cdc_lsn, _cdc_updated_at)
        SELECT id, customer_id, amount, status, created_at, 'snapshot', now()
        FROM pg.orders
    """)
    n = con.execute("SELECT count(*) FROM orders").fetchone()[0]
    dt = time.monotonic() - t0
    con.close()

    print(f"  Snapshot complete: {n:,} rows in {dt:.1f}s")
    print("  Now run:  python src/cdc_consumer.py   (streams everything after the slot LSN)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Bulk-load pre-slot rows into the mirror")
    p.add_argument("--no-truncate", dest="truncate", action="store_false",
                   help="append instead of clearing the mirror first")
    args = p.parse_args()
    snapshot(args.truncate)
