"""Phase 1 — the NAIVE pipeline. Works once. Run it twice and watch it break.

extract() pulls a day's orders from Postgres, transform() aggregates them with
DuckDB, load() appends to the analytical target. There is no idempotency here:
every run blindly INSERTs, so the second run duplicates every (date, status) row.

Usage:
    python src/pipeline_naive.py 2024-01-15
    python src/pipeline_naive.py 2024-01-15   # run again → duplicates
"""

import sys
from datetime import date, datetime

import duckdb
import psycopg

from config import PG_DSN, connect_target


def extract(target_date: date) -> list[dict]:
    """Extract orders for a single date from the OLTP source. Explicit columns,
    never SELECT * — a renamed/dropped column should fail loudly, not silently."""
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, customer_id, amount, status, created_at
            FROM orders
            WHERE created_at::date = %s
            """,
            (target_date,),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def transform(raw_orders: list[dict], target_date: date) -> list[tuple]:
    """Aggregate to daily revenue by status, in DuckDB. We materialize the
    extracted rows into a temp `raw` table, then let DuckDB do the GROUP BY."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE raw (id BIGINT, customer_id INT, amount DECIMAL(10,2), "
        "status TEXT, created_at TIMESTAMPTZ)"
    )
    con.executemany(
        "INSERT INTO raw VALUES (?, ?, ?, ?, ?)",
        [(r["id"], r["customer_id"], r["amount"], r["status"], r["created_at"])
         for r in raw_orders],
    )
    return con.execute(
        """
        SELECT
            ?::DATE          AS date,
            status,
            SUM(amount)      AS total_revenue,
            COUNT(*)         AS order_count,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM raw
        GROUP BY status
        ORDER BY status
        """,
        [target_date],
    ).fetchall()


def load(rows: list[tuple]) -> None:
    """NAIVE load: blind append. No DELETE, no UPSERT → re-runs duplicate."""
    con = connect_target()
    for r in rows:
        con.execute(
            """
            INSERT INTO daily_revenue
                (date, status, total_revenue, order_count, avg_order_value, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (*r, datetime.now()),
        )
    con.close()


def run_pipeline(target_date: date) -> None:
    print(f"Extracting orders for {target_date}...")
    con = connect_target()
    before = con.execute("SELECT COUNT(*) FROM daily_revenue WHERE date = ?", (target_date,)).fetchone()[0]
    print(f"  Before: {before} rows for {target_date}")
    con.close()

    raw = extract(target_date)
    print(f"  Extracted {len(raw):,} rows")
    print("Transforming...")
    agg = transform(raw, target_date)
    print(f"  Produced {len(agg)} aggregated rows")
    print("Loading (naive append)...")
    load(agg)

    con = connect_target()
    after = con.execute("SELECT COUNT(*) FROM daily_revenue WHERE date = ?", (target_date,)).fetchone()[0]
    print(f"  After:  {after} rows for {target_date}")
    if after == before + len(agg):
        print(f"  ⚠️  DUPLICATION: +{len(agg)} rows (naive append)")
    elif after == before:
        print(f"  ✓ NO CHANGE: {after} rows (idempotent)")
    else:
        print(f"  ? UNEXPECTED: {before} → {after}")
    con.close()


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2024, 1, 15)
    run_pipeline(d)
