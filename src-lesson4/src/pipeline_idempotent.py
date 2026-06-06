"""Phase 2 — make the load IDEMPOTENT. Run it N times, get the result of running
it once. Two equivalent strategies are shown; pick one with --strategy.

  delete-insert  DELETE the target partition, then INSERT, both in ONE transaction.
                 Works without a primary key; explicit about what happens.
  upsert         INSERT OR REPLACE keyed on (date, status). Concise; needs a PK.

Usage:
    python src/pipeline_idempotent.py 2024-01-15
    python src/pipeline_idempotent.py 2024-01-15 --strategy upsert
"""

import argparse
from datetime import date, datetime

from config import connect_target
from pipeline_naive import extract, transform


def load_delete_insert(rows: list[tuple], target_date: date) -> None:
    """Partition replacement: blow away the date, rewrite it, atomically.

    The DELETE and the INSERTs share one transaction. If anything throws, the
    ROLLBACK also undoes the DELETE — the target is never left half-empty."""
    con = connect_target()
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
        for r in rows:
            con.execute(
                """
                INSERT INTO daily_revenue
                    (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (*r, datetime.now()),
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def load_upsert(rows: list[tuple], target_date: date) -> None:
    """UPSERT: re-inserting the same key overwrites with the same value → idempotent.
    Targets daily_revenue_keyed, which declares PRIMARY KEY (date, status) — the
    conflict target INSERT OR REPLACE needs."""
    con = connect_target()
    con.execute("BEGIN TRANSACTION")
    try:
        for r in rows:
            con.execute(
                """
                INSERT OR REPLACE INTO daily_revenue_keyed
                    (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (*r, datetime.now()),
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def run_pipeline(target_date: date, strategy: str = "delete-insert") -> None:
    print(f"[{strategy}] {target_date}")
    con = connect_target()
    table = "daily_revenue_keyed" if strategy == "upsert" else "daily_revenue"
    before = con.execute(f"SELECT COUNT(*) FROM {table} WHERE date = ?", (target_date,)).fetchone()[0]
    print(f"  Before: {before} rows for {target_date}")
    con.close()

    raw = extract(target_date)
    agg = transform(raw, target_date)
    if strategy == "upsert":
        load_upsert(agg, target_date)
    else:
        load_delete_insert(agg, target_date)

    con = connect_target()
    after = con.execute(f"SELECT COUNT(*) FROM {table} WHERE date = ?", (target_date,)).fetchone()[0]
    print(f"  After:  {after} rows for {target_date}")
    print(f"  Loaded {len(agg)} rows ({len(raw):,} source orders)")
    if after == before + len(agg):
        print(f"  ⚠️  DUPLICATION: +{len(agg)} rows (naive append)")
    elif after == len(agg):
        print(f"  ✓ IDEMPOTENT: {after} rows (partition rewrite)")
    con.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("target_date", nargs="?", default="2024-01-15")
    p.add_argument("--strategy", choices=["delete-insert", "upsert"],
                   default="delete-insert")
    args = p.parse_args()
    run_pipeline(date.fromisoformat(args.target_date), args.strategy)
