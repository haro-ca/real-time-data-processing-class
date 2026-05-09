"""Phase 1 — Naive: single connection, synchronous commits, one INSERT at a time.

Usage:
    python load_naive.py [--rows 100000]
"""

import argparse
import random
import time

import psycopg

DSN = "dbname=bench user=bench password=bench host=localhost port=5432"

INSERT_SQL = """
    INSERT INTO orders (customer_id, amount)
    VALUES (%s, %s)
"""


def run(total_rows: int) -> None:
    conn = psycopg.connect(DSN, autocommit=True)
    cur = conn.cursor()

    print(f"Phase 1 — Naive loader: {total_rows:,} rows, 1 connection, autocommit")
    print("-" * 60)

    done = 0
    t0 = time.monotonic()
    last_report = t0

    for i in range(total_rows):
        cur.execute(INSERT_SQL, (random.randint(1, 10_000), round(random.uniform(1, 500), 2)))
        done += 1

        now = time.monotonic()
        if now - last_report >= 1.0:
            elapsed = now - t0
            tps = done / elapsed
            print(f"  [{elapsed:6.1f}s]  {tps:,.0f} TPS  |  total: {done:,}")
            last_report = now

    elapsed = time.monotonic() - t0
    tps = done / elapsed
    print("-" * 60)
    print(f"Done. {done:,} rows in {elapsed:.1f}s → {tps:,.0f} TPS")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 1 — Naive sync loader")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows to insert")
    args = parser.parse_args()
    run(args.rows)
