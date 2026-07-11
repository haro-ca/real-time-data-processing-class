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


def produced_from_file():
    if not PRODUCED_FILE.exists():
        return None
    data = json.loads(PRODUCED_FILE.read_text())
    return data.get("total")


def main():
    banner("verify", "checking the Postgres sink")
    total, duplicates, missing_enrichment = postgres_counts()
    produced = produced_from_file()

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

    if produced is not None and total == produced:
        print(f"  ✓ Postgres count matches produced count ({total})")
    elif produced is not None:
        print(f"  ✗ Postgres count ({total}) != produced count ({produced})")
        sys.exit(1)

    if duplicates or missing_enrichment:
        sys.exit(1)


if __name__ == "__main__":
    main()
