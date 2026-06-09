"""Seed the OLTP source: ~1M orders over a date window + ~50k customers.

Same DuckDB -> Postgres server-side COPY trick as Lesson 4 (1M rows in seconds),
with one change: orders also gets `updated_at` (= created_at at seed time) so the
naive polling demo has a column to poll on.

IMPORTANT for the CDC story: seed BEFORE you create the slot. Everything seeded
here lands in the WAL *before* setup_cdc.py opens the slot, so it will NOT appear
in the change stream — that is exactly the "initial snapshot" problem snapshot.py
solves. Order: seed_data.py -> setup_cdc.py -> (mutations stream) -> snapshot.py.

Usage:
    python src/seed_data.py                       # 1M orders, 50k customers, 90 days
    python src/seed_data.py --orders 200000 --days 30
"""

import argparse
import time

import duckdb

from config import PG_CONN_STR

START_DATE = "2024-01-01"
CITIES = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Seattle", "Miami", "Denver"]
REGIONS = {"NYC": "East", "Miami": "East", "Chicago": "Central", "Houston": "Central",
           "Denver": "Central", "LA": "West", "Phoenix": "West", "Seattle": "West"}


def seed(n_orders: int, n_customers: int, n_days: int) -> None:
    print("Seeding OLTP source (Postgres)...")
    print(f"  customers: {n_customers:,}")
    print(f"  orders:    {n_orders:,} across {n_days} days from {START_DATE}")

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres")
    con.sql(f"ATTACH '{PG_CONN_STR}' AS pg (TYPE postgres)")

    # Reset so re-seeding is itself idempotent.
    con.sql("CALL postgres_execute('pg', 'TRUNCATE orders, customers')")

    cities_sql = "[" + ", ".join(f"'{c}'" for c in CITIES) + "]"
    region_case = " ".join(f"WHEN '{c}' THEN '{REGIONS[c]}'" for c in CITIES)

    t0 = time.monotonic()
    print("  Generating customers...")
    con.sql(f"""
        INSERT INTO pg.customers (id, name, city, region, signup_date, updated_at)
        SELECT
            i AS id,
            'Customer ' || i AS name,
            city,
            CASE city {region_case} END AS region,
            DATE '2020-01-01' + (random() * 1400)::INT AS signup_date,
            now()
        FROM (
            SELECT i, {cities_sql}[(floor(random() * {len(CITIES)})::INT + 1)] AS city
            FROM range(1, {n_customers + 1}) t(i)
        )
    """)

    print("  Generating orders...")
    # updated_at = created_at at seed time: a clean baseline for poll_sync.
    con.sql(f"""
        INSERT INTO pg.orders (customer_id, amount, status, created_at, updated_at)
        SELECT customer_id, amount, status, created_at, created_at
        FROM (
            SELECT
                (floor(random() * {n_customers})::INT + 1)                          AS customer_id,
                round(5 + random() * 495, 2)::DECIMAL(10,2)                         AS amount,
                ['pending', 'shipped', 'delivered'][(floor(random() * 3)::INT + 1)] AS status,
                TIMESTAMP '{START_DATE}' + (random() * {n_days * 86400})::INT * INTERVAL 1 SECOND AS created_at
            FROM range({n_orders})
        )
    """)

    con.sql("CALL postgres_execute('pg', 'ANALYZE orders')")
    con.sql("CALL postgres_execute('pg', 'ANALYZE customers')")
    con.close()

    dt = time.monotonic() - t0
    print(f"  Done in {dt:.1f}s ({n_orders / dt:,.0f} orders/sec)")
    print(f"  Date range: {START_DATE} .. +{n_days} days")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Seed OLTP orders + customers")
    p.add_argument("--orders", type=int, default=1_000_000)
    p.add_argument("--customers", type=int, default=50_000)
    p.add_argument("--days", type=int, default=90)
    args = p.parse_args()
    seed(args.orders, args.customers, args.days)
