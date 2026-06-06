"""Data quality — assert value-level invariants the schema contract can't see.

schema_validate.py checks the SHAPE (which columns exist, what types). This checks
the VALUES. A column can keep its name and type while its contents go wrong: a
negative amount, an unknown status, an aggregate that doesn't reconcile. Those
slip past a schema check and silently corrupt the numbers — the worst kind of
drift, because nothing crashes. Run this after a load and fail loud.

Checks, for one target date:
    source · orders          amount > 0, customer_id not null, status in known set
    target · daily_revenue   one row per status, order_count > 0, revenue >= 0,
                             avg_order_value reconciles with total / count

Exit code is non-zero if any check fails, so it drops straight into a pipeline.

Usage:
    python src/data_quality.py 2024-01-15
"""

import argparse
import sys
from datetime import date

import psycopg

from config import PG_DSN, connect_target

KNOWN_STATUSES = {"pending", "shipped", "delivered"}


def check(label: str, ok: bool, detail: str = "") -> bool:
    mark = "OK  " if ok else "FAIL"
    print(f"  {mark}  {label}{(' — ' + detail) if detail else ''}")
    return ok


def source_checks(target_date: date) -> list[bool]:
    """Value invariants on the raw source rows for the date."""
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE amount <= 0),
                   COUNT(*) FILTER (WHERE customer_id IS NULL)
            FROM orders WHERE created_at::date = %s
            """,
            (target_date,),
        )
        total, bad_amount, null_cust = cur.fetchone()
        cur.execute(
            "SELECT DISTINCT status FROM orders WHERE created_at::date = %s",
            (target_date,),
        )
        statuses = {r[0] for r in cur.fetchall()}

    unknown = statuses - KNOWN_STATUSES
    return [
        check(f"orders present for {target_date}", total > 0, f"{total:,} rows"),
        check("amount > 0", bad_amount == 0, f"{bad_amount} non-positive"),
        check("customer_id not null", null_cust == 0, f"{null_cust} null"),
        check("status in known set", not unknown,
              f"unknown: {sorted(unknown)}" if unknown else "pending/shipped/delivered"),
    ]


def target_checks(target_date: date) -> list[bool]:
    """Reconciliation invariants on the loaded aggregate for the date."""
    con = connect_target()
    rows = con.execute(
        """
        SELECT status, total_revenue, order_count, avg_order_value
        FROM daily_revenue WHERE date = ?
        """,
        (target_date,),
    ).fetchall()
    con.close()

    if not rows:
        return [check(f"daily_revenue loaded for {target_date}", False,
                      "no rows — run the pipeline first")]

    statuses = {r[0] for r in rows}
    unknown = statuses - KNOWN_STATUSES
    zero_count = [r[0] for r in rows if r[2] <= 0]
    neg_revenue = [r[0] for r in rows if r[1] < 0]
    # avg should equal total / count to within rounding (avg is ROUND(.., 2)).
    off_avg = [r[0] for r in rows if abs(float(r[3]) - float(r[1]) / r[2]) > 0.01]

    return [
        check(f"daily_revenue loaded for {target_date}", True, f"{len(rows)} status rows"),
        check("status in known set", not unknown,
              f"unknown: {sorted(unknown)}" if unknown else ""),
        check("order_count > 0", not zero_count,
              f"zero on: {zero_count}" if zero_count else ""),
        check("total_revenue >= 0", not neg_revenue,
              f"negative on: {neg_revenue}" if neg_revenue else ""),
        check("avg_order_value reconciles", not off_avg,
              f"off on: {off_avg}" if off_avg else "= total / count"),
    ]


def main(target_date: date) -> int:
    print(f"Data quality for {target_date}\n")
    print("  source · orders")
    src = source_checks(target_date)
    print("\n  target · daily_revenue")
    tgt = target_checks(target_date)

    print()
    if all(src + tgt):
        print("  ALL CHECKS PASSED — values are trustworthy.")
        return 0
    print("  DATA QUALITY FAILURES — do not trust this partition.")
    return 1


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Value-level data-quality checks")
    p.add_argument("target_date", nargs="?", default="2024-01-15")
    args = p.parse_args()
    sys.exit(main(date.fromisoformat(args.target_date)))
