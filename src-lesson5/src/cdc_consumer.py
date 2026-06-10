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

Schema drift: if the source ships a column the mirror doesn't have, applying the
event anyway would silently DROP that column — the mirror would lie, the exact
failure polling had. Default is to fail loud (and since the slot was not advanced,
nothing is lost: fix the mirror, rerun, the event replays). --ignore-drift keeps
the old silent behavior, for the demo's "before" act.

Usage:
    python src/cdc_consumer.py                     # stream forever (Ctrl-C to stop)
    python src/cdc_consumer.py --once              # drain what's pending, then exit
    python src/cdc_consumer.py --limit 50          # apply 50 changes then exit
    python src/cdc_consumer.py --crash-after 20    # apply 20, exit BEFORE confirming
    python src/cdc_consumer.py --ignore-drift      # silently drop unknown columns
"""

import argparse
import json
import sys
import time

import psycopg

from config import PG_DSN, SLOT, connect_target, mirror_checksum

# wal2json format-version 2 = one JSON object per change, wrapped in begin/commit
# markers (action "B"/"C"). We KEEP those markers on purpose: the commit ("C") row
# carries the transaction's commit LSN, and that commit LSN is the only safe point
# to confirm to. A change row's own LSN comes *before* its commit, so confirming to
# it never advances past the last transaction — the slot would re-deliver forever.
# add-tables filters to orders (wal2json ignores the publication — that's a
# pgoutput/Debezium concept).
PEEK_SQL = """
    SELECT lsn::text, data
    FROM pg_logical_slot_peek_changes(%s, NULL, %s,
           'format-version', '2',
           'add-tables', 'public.orders')
"""


def cols_to_dict(items) -> dict:
    """wal2json columns/identity -> {name: value}."""
    return {c["name"]: c.get("value") for c in (items or [])}


# Every source column the consumer KNOWS about — the five it mirrors plus
# updated_at, which exists on the source only for the polling demo and is
# deliberately not mirrored. Anything beyond these arriving in an event is
# schema drift: a column we never decided what to do with.
KNOWN_COLUMNS = {"id", "customer_id", "amount", "status", "created_at", "updated_at"}


class SchemaDrift(RuntimeError):
    """The source shipped a column the mirror doesn't have."""


def apply(duck, ev: dict, lsn: str, strict: bool = True) -> None:
    """Apply one change event to the DuckDB mirror, idempotently, in one txn.

    delete-then-insert keyed on the primary key. The DELETE runs for I, U *and* D,
    so re-applying any event is a no-op: an INSERT replayed after a crash deletes
    the row it already wrote and writes it again, instead of failing on a duplicate
    primary key. CDC is at-least-once; this is exactly what makes replay harmless.
    """
    action = ev["action"]                       # I / U / D
    if action in ("U", "D"):
        ident = cols_to_dict(ev.get("identity")) or cols_to_dict(ev.get("columns"))
        row_id = ident["id"]
    else:
        row_id = cols_to_dict(ev["columns"])["id"]

    # Schema drift check BEFORE touching the mirror: a column the mirror doesn't
    # have can't be applied, only silently dropped — and silent is the one thing
    # this lesson never forgives. Fail loud; the slot wasn't advanced, so nothing
    # is lost: ALTER the mirror (or pass --ignore-drift) and rerun.
    if strict and action in ("I", "U"):
        drift = sorted(set(cols_to_dict(ev["columns"])) - KNOWN_COLUMNS)
        if drift:
            raise SchemaDrift(
                f"source sent column(s) {drift} the mirror doesn't have (lsn {lsn}).\n"
                f"  slot NOT advanced — zero events lost.\n"
                f"  fix:  ALTER TABLE orders ADD COLUMN ... on the mirror, update "
                f"KNOWN_COLUMNS, rerun\n"
                f"  or:   rerun with --ignore-drift to drop them silently (the mirror will lie)"
            )

    duck.execute("BEGIN")
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
        batch: int, strict: bool = True) -> None:
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

            # wal2json brackets each txn with begin/commit markers (action B/C).
            # Apply only I/U/D; remember the latest commit LSN we fully applied —
            # that's the safe point to confirm to.
            last_commit_lsn = None
            n_changes = 0
            for lsn, data in rows:
                ev = json.loads(data)
                action = ev["action"]
                if action == "C":            # commit marker => a safe confirm point
                    last_commit_lsn = lsn
                    continue
                if action == "B":            # begin marker => nothing to apply
                    continue

                apply(duck, ev, lsn, strict)
                applied += 1
                n_changes += 1

                # Simulate a crash AFTER applying but BEFORE confirming: nothing was
                # advanced, so the next run re-peeks the whole transaction and
                # re-applies it (no-op, idempotent).
                if crash_after is not None and applied >= crash_after:
                    n, chk = mirror_checksum(duck)
                    print(f"  applied {applied} events ... ^C  (simulated crash before confirm)")
                    print(f"  mirror now {n} rows, checksum {chk}  (slot NOT advanced)")
                    sys.exit(1)

                if limit is not None and applied >= limit:
                    break

            # CONFIRM: advance the slot to the last COMMIT we durably applied.
            if last_commit_lsn is not None:
                pg.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)",
                           (SLOT, last_commit_lsn))

            n, chk = mirror_checksum(duck)
            print(f"  +{n_changes:<4} applied  (total {applied})  confirmed {last_commit_lsn}  "
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
    p.add_argument("--ignore-drift", action="store_true",
                   help="silently drop source columns the mirror doesn't have")
    args = p.parse_args()
    try:
        run(args.once, args.limit, args.crash_after, args.interval, args.batch,
            strict=not args.ignore_drift)
    except KeyboardInterrupt:
        print("\nstopped.")
    except SchemaDrift as e:
        print(f"\nSCHEMA DRIFT — failing loud, on purpose.\n{e}", file=sys.stderr)
        sys.exit(2)
