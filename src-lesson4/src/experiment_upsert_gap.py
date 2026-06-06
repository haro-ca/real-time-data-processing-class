"""Experiment — UPSERT leaves orphaned keys; DELETE + INSERT doesn't.

Both strategies are idempotent when the SAME keys reappear every run. They part
ways when a key *disappears* from the source:

  DELETE + INSERT  rewrites the whole partition → a vanished key is dropped.
  UPSERT           only touches keys it sees → a vanished key's stale row SURVIVES.

Real daily data always has all three statuses, so the gap never shows up by
accident. This demo forces it: load three statuses, then re-load the same date
with one status gone, and compare the two target tables side by side.

It writes to a throwaway demo date (year 2099) and cleans up after itself, so it
never touches your real loads.

Usage:
    python src/experiment_upsert_gap.py
"""

import argparse
from datetime import date

from config import connect_target
from pipeline_idempotent import load_delete_insert, load_upsert

DEMO_DATE = date(2099, 1, 1)


def rows_for(statuses: list[str]) -> list[tuple]:
    """Synthetic aggregate rows: (date, status, total_revenue, order_count, avg)."""
    return [(DEMO_DATE, s, 100.0 * (i + 1), 10 * (i + 1), 10.0)
            for i, s in enumerate(statuses)]


def snapshot(table: str) -> list[str]:
    con = connect_target()
    rows = con.execute(
        f"SELECT status FROM {table} WHERE date = ? ORDER BY status", (DEMO_DATE,)
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def cleanup() -> None:
    con = connect_target()
    con.execute("DELETE FROM daily_revenue WHERE date = ?", (DEMO_DATE,))
    con.execute("DELETE FROM daily_revenue_keyed WHERE date = ?", (DEMO_DATE,))
    con.close()


def run() -> None:
    cleanup()

    full = ["delivered", "pending", "shipped"]
    shrunk = ["delivered", "pending"]  # 'shipped' disappears on the re-run

    # Run 1 — all three statuses present.
    load_delete_insert(rows_for(full), DEMO_DATE)
    load_upsert(rows_for(full), DEMO_DATE)

    # Run 2 — same date, but 'shipped' is gone from the source.
    load_delete_insert(rows_for(shrunk), DEMO_DATE)
    load_upsert(rows_for(shrunk), DEMO_DATE)

    di = snapshot("daily_revenue")
    up = snapshot("daily_revenue_keyed")

    print(f"Re-loaded {DEMO_DATE} with 'shipped' removed from the source.\n")
    print(f"  DELETE + INSERT  daily_revenue        -> {di}")
    print(f"  UPSERT           daily_revenue_keyed  -> {up}\n")

    orphan = set(up) - set(di)
    if orphan:
        print(f"  ORPHANED KEY: UPSERT still carries {sorted(orphan)} — a stale row "
              f"the source no longer has.")
        print("  DELETE + INSERT dropped it, because it rewrites the whole partition.")
        print("  Lesson: prefer DELETE + INSERT unless every key is guaranteed to reappear.")
    else:
        print("  No orphan detected (unexpected for this demo).")

    cleanup()


if __name__ == "__main__":
    argparse.ArgumentParser(description="Demonstrate the UPSERT orphaned-key gap").parse_args()
    run()
