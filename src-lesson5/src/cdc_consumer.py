"""The CDC consumer: stream wal2json changes from the slot into a DuckDB mirror.

Loop:  peek the slot  ->  apply each change (idempotent)  ->  advance the slot.

This is the "apply, THEN confirm" discipline from the slides. We use plain SQL:

    pg_logical_slot_peek_changes()   reads pending changes WITHOUT advancing
    <apply to DuckDB>
    pg_replication_slot_advance()    moves confirmed_flush_lsn forward = "confirm"

If we crash between apply and advance, the next run re-peeks the same changes and
re-applies them. That's at-least-once delivery, and it's harmless ONLY because the
apply is idempotent: UPDATE/DELETE+INSERT keyed on id (the same trick as Lesson 4).

(Production tools like Debezium use the streaming replication protocol with
send_feedback() instead of peek+advance. Same idea — confirm an LSN after you've
durably applied — with more plumbing. We read JSON so the lesson stays about CDC.)

Usage:
    python src/cdc_consumer.py                     # stream forever (Ctrl-C to stop)
    python src/cdc_consumer.py --once              # drain what's pending, then exit
    python src/cdc_consumer.py --limit 50          # apply 50 changes then exit
    python src/cdc_consumer.py --crash-after 20    # apply 20, exit BEFORE confirming
"""

import argparse
import json
import sys
import time

import psycopg

from config import PG_DSN, SLOT, connect_target, mirror_checksum

# wal2json format-version 2 = one JSON object per change. include-transaction=false
# drops begin/commit markers; add-tables filters to orders (wal2json ignores the
# publication — that's a pgoutput/Debezium concept).
PEEK_SQL = """
    SELECT lsn::text, data
    FROM pg_logical_slot_peek_changes(%s, NULL, %s,
           'format-version', '2',
           'include-transaction', 'false',
           'add-tables', 'public.orders')
"""


def cols_to_dict(items) -> dict:
    """wal2json columns/identity -> {name: value}."""
    return {c["name"]: c.get("value") for c in (items or [])}


def apply(duck, ev: dict, lsn: str) -> None:
    """Apply one change event to the DuckDB mirror, idempotently, in one txn."""
    action = ev["action"]                       # I / U / D
    if action in ("U", "D"):
        ident = cols_to_dict(ev.get("identity")) or cols_to_dict(ev.get("columns"))
        row_id = ident["id"]
    else:
        row_id = cols_to_dict(ev["columns"])["id"]

    duck.execute("BEGIN")
    if action in ("U", "D"):
        duck.execute("DELETE FROM orders WHERE id = ?", [row_id])
    if action in ("I", "U"):
        c = cols_to_dict(ev["columns"])
        duck.execute(
            """INSERT INTO orders (id, customer_id, amount, status, created_at, _cdc_lsn)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [c["id"], c.get("customer_id"), c.get("amount"), c.get("status"),
             c.get("created_at"), lsn],
        )
    duck.execute("COMMIT")


def run(once: bool, limit: int | None, crash_after: int | None, interval: float,
        batch: int) -> None:
    duck = connect_target()
    applied = 0
    print(f"CDC consumer on slot '{SLOT}'. Peek -> apply -> advance. Ctrl-C to stop.\n")

    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        while True:
            rows = pg.execute(PEEK_SQL, (SLOT, batch)).fetchall()
            if not rows:
                if once:
                    break
                time.sleep(interval)
                continue

            last_lsn = None
            for lsn, data in rows:
                apply(duck, json.loads(data), lsn)
                last_lsn = lsn
                applied += 1

                # Simulate a crash AFTER applying but BEFORE confirming: the
                # changes we just applied were never advanced, so the next run
                # re-peeks and re-applies them (no-op, idempotent).
                if crash_after is not None and applied >= crash_after:
                    n, chk = mirror_checksum(duck)
                    print(f"  applied {applied} events ... ^C  (simulated crash before confirm)")
                    print(f"  mirror now {n} rows, checksum {chk}  (slot NOT advanced)")
                    sys.exit(1)

                if limit is not None and applied >= limit:
                    break

            # CONFIRM: advance the slot to the last change we durably applied.
            pg.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, last_lsn))

            n, chk = mirror_checksum(duck)
            print(f"  +{len(rows):<4} applied  (total {applied})  confirmed {last_lsn}  "
                  f"mirror {n} rows  checksum {chk}")

            if limit is not None and applied >= limit:
                break

    n, chk = mirror_checksum(duck)
    duck.close()
    print(f"\nDone. mirror = {n} rows, checksum {chk}.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Stream wal2json CDC into DuckDB")
    p.add_argument("--once", action="store_true", help="drain pending changes then exit")
    p.add_argument("--limit", type=int, help="apply N changes then exit")
    p.add_argument("--crash-after", type=int, help="apply N then exit BEFORE confirming (replay demo)")
    p.add_argument("--interval", type=float, default=0.5, help="poll interval when idle (s)")
    p.add_argument("--batch", type=int, default=5000, help="max changes per peek")
    args = p.parse_args()
    try:
        run(args.once, args.limit, args.crash_after, args.interval, args.batch)
    except KeyboardInterrupt:
        print("\nstopped.")
