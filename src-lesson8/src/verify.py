"""Verify the Lesson 8 sink.

Run it before the kill to record a baseline, and after the restart to prove the
pipeline is exactly-once end-to-end."""

import json
import sys

import psycopg

from config import POSTGRES_URL, PRODUCED_FILE, banner


def postgres_counts():
    conn = psycopg.connect(POSTGRES_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM enriched_transactions")
        total = cur.fetchone()[0]
        cur.execute("""
            SELECT transaction_id, COUNT(*)
            FROM enriched_transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        """)
        duplicates = len(cur.fetchall())
        cur.execute("""
            SELECT COUNT(*) FROM enriched_transactions
            WHERE customer_name IS NULL
        """)
        missing_enrichment = cur.fetchone()[0]
    conn.close()
    return total, duplicates, missing_enrichment


def aggregate_check():
    """Compare the streaming rollup against a batch recomputation.

    Only windows that closed at least 2 minutes ago are compared, so the
    watermark has passed and both queries have had time to catch up. Returns
    None when phase 5 (streaming_aggregate.py) has not been run yet."""
    conn = psycopg.connect(POSTGRES_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('customer_activity')")
        if cur.fetchone()[0] is None:
            conn.close()
            return None
        cur.execute("SELECT COUNT(*) FROM customer_activity")
        if cur.fetchone()[0] == 0:
            conn.close()
            return None
        cur.execute("""
            WITH expected AS (
                SELECT customer_id,
                       date_trunc('minute', transaction_time) AS window_start,
                       COUNT(*) AS txn_count,
                       SUM(amount) AS total_amount
                FROM enriched_transactions
                WHERE date_trunc('minute', transaction_time)
                      < now() - interval '2 minutes'
                GROUP BY 1, 2
            )
            SELECT
                COUNT(*) FILTER (WHERE a.customer_id IS NULL) AS missing,
                COUNT(*) FILTER (WHERE a.customer_id IS NOT NULL
                                 AND (a.txn_count <> e.txn_count
                                      OR a.total_amount <> e.total_amount))
                    AS mismatched,
                COUNT(*) AS windows
            FROM expected e
            LEFT JOIN customer_activity a
                   ON a.customer_id = e.customer_id
                  AND a.window_start = e.window_start
        """)
        missing, mismatched, windows = cur.fetchone()
    conn.close()
    return {"windows": windows, "missing": missing, "mismatched": mismatched}


def produced_from_file():
    if not PRODUCED_FILE.exists():
        return None
    data = json.loads(PRODUCED_FILE.read_text())
    return data.get("total")


def main():
    banner("verify", "checking the Postgres sink")
    total, duplicates, missing_enrichment = postgres_counts()
    produced = produced_from_file()
    agg = aggregate_check()

    print(f"  Postgres rows:            {total}")
    print(f"  Duplicates:               {duplicates}")
    print(f"  Missing enrichment:       {missing_enrichment}")
    print(f"  Produced count (file):    {produced if produced is not None else 'not recorded'}")

    if duplicates == 0:
        print("\n  ✓ no duplicate transaction_ids")
    else:
        print(f"\n  ✗ found {duplicates} duplicated transaction_ids")

    if missing_enrichment == 0:
        print("  ✓ all rows have customer enrichment")
    else:
        print(f"  ✗ {missing_enrichment} rows have no customer enrichment")

    failed = False
    if produced is not None and total == produced:
        print(f"  ✓ Postgres count matches produced count ({total})")
    elif produced is not None:
        print(f"  ✗ Postgres count ({total}) != produced count ({produced})")
        failed = True

    if agg is None:
        print("  - aggregate check skipped (customer_activity empty; phase 5 not run)")
    else:
        print(f"\n  Aggregate windows checked:  {agg['windows']}")
        print(f"  Missing windows:            {agg['missing']}")
        print(f"  Mismatched sums/counts:     {agg['mismatched']}")
        if agg["missing"] == 0 and agg["mismatched"] == 0:
            print("  ✓ streaming aggregates match batch recomputation exactly")
        else:
            print("  ✗ streaming aggregates diverge from batch recomputation")
            failed = True

    if failed or duplicates or missing_enrichment:
        sys.exit(1)


if __name__ == "__main__":
    main()
