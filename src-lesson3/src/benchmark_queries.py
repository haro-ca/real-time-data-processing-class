"""Head-to-head benchmark: 4 queries run in both Postgres and DuckDB.

Measures wall-clock time and reports the ratio. This is the core practical for
Lesson 3 — students see the gap, then must explain it.

The Postgres query is the source of truth. For DuckDB, the SQL is templated:
the {table} placeholder becomes the parquet glob, and the parquet column names
that differ from Postgres (tpep_pickup_datetime → pickup_datetime, PULocationID
→ pickup_location_id, etc.) are aliased in a subquery so the rest of the SQL
reads identically across engines.

Usage:
    uv run python benchmark_queries.py [--pg-only | --duck-only]
"""

import argparse
import os
import time
from pathlib import Path

import duckdb
import psycopg

DATA_DIR = Path(__file__).parent / "data"
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:{PG_PORT}/bench"
PARQUET_GLOB = str(DATA_DIR / "yellow_tripdata_*.parquet")

# DuckDB sees the Parquet files via a templated subquery that renames the raw
# parquet columns to match the Postgres schema. The benchmark SQL then uses the
# Postgres-style names in both engines — same query, different storage.
DUCK_TABLE = f"""(
    SELECT
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        PULocationID          AS pickup_location_id,
        DOLocationID          AS dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        trip_distance
    FROM read_parquet('{PARQUET_GLOB}', union_by_name=true)
)"""

QUERIES = [
    {
        "name": "Q1: Full aggregation (I/O bound)",
        "sql": """
            SELECT COUNT(*), AVG(fare_amount), AVG(tip_amount), AVG(trip_distance)
            FROM {table}
        """,
    },
    {
        "name": "Q2: Filtered aggregation (zone maps)",
        "sql": """
            SELECT DATE_TRUNC('month', pickup_datetime) AS month,
                   payment_type,
                   COUNT(*) AS trips,
                   AVG(fare_amount) AS avg_fare,
                   SUM(tip_amount) AS total_tips
            FROM {table}
            WHERE pickup_datetime >= '2025-02-01'
              AND pickup_datetime < '2025-03-01'
            GROUP BY month, payment_type
            ORDER BY month, payment_type
        """,
    },
    {
        "name": "Q3: High-cardinality GROUP BY",
        "sql": """
            SELECT pickup_location_id, dropoff_location_id,
                   COUNT(*) AS trips,
                   AVG(fare_amount) AS avg_fare
            FROM {table}
            GROUP BY pickup_location_id, dropoff_location_id
            ORDER BY trips DESC
            LIMIT 20
        """,
    },
    {
        "name": "Q4: Window function (CPU bound)",
        "sql": """
            SELECT pickup_location_id, pickup_datetime, fare_amount,
                   AVG(fare_amount) OVER (
                       PARTITION BY pickup_location_id
                       ORDER BY pickup_datetime
                       ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg
            FROM {table}
            WHERE pickup_location_id = 132
            ORDER BY pickup_datetime
        """,
    },
]


def run_postgres(sql: str) -> float:
    """Run query in Postgres, return wall-clock seconds."""
    with psycopg.connect(PG_DSN) as conn:
        t0 = time.monotonic()
        with conn.cursor() as cur:
            cur.execute(sql.format(table="trips"))
            cur.fetchall()
        return time.monotonic() - t0


def run_duckdb(con: duckdb.DuckDBPyConnection, sql: str) -> float:
    """Run query in DuckDB, return wall-clock seconds."""
    t0 = time.monotonic()
    con.sql(sql.format(table=DUCK_TABLE)).fetchall()
    return time.monotonic() - t0


def main(pg_only: bool = False, duck_only: bool = False) -> None:
    print("=" * 70)
    print("  Lesson 3 — DuckDB vs Postgres: Head-to-Head Benchmark")
    print("=" * 70)
    print()

    duck_con = duckdb.connect()
    # No explicit memory_limit / threads: the comparison's fairness comes from
    # running this script inside the duckdb container, which has the same
    # cgroup cap as Postgres (4 CPU / 8 GB). When students run natively on
    # host with `uv run python ...`, DuckDB uses 80% of host RAM — that's the
    # "unconstrained reveal" at the end of the lesson.

    results = []

    for q in QUERIES:
        print(f"  {q['name']}")
        print(f"  {'-' * 50}")

        pg_time = None
        duck_time = None

        if not duck_only:
            try:
                pg_time = run_postgres(q["sql"])
                print(f"    Postgres:  {pg_time:.3f}s")
            except Exception as e:
                print(f"    Postgres:  ERROR — {e}")

        if not pg_only:
            try:
                duck_time = run_duckdb(duck_con, q["sql"])
                print(f"    DuckDB:    {duck_time:.3f}s")
            except Exception as e:
                print(f"    DuckDB:    ERROR — {e}")

        if pg_time and duck_time:
            ratio = pg_time / duck_time
            print(f"    Ratio:     {ratio:.1f}× faster in DuckDB")

        results.append({"name": q["name"], "pg": pg_time, "duck": duck_time})
        print()

    duck_con.close()

    # Summary table
    print("=" * 70)
    print(f"  {'Query':<40} {'Postgres':>10} {'DuckDB':>10} {'Ratio':>8}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*8}")
    for r in results:
        pg_str = f"{r['pg']:.2f}s" if r["pg"] else "—"
        duck_str = f"{r['duck']:.3f}s" if r["duck"] else "—"
        ratio_str = f"{r['pg']/r['duck']:.0f}×" if r["pg"] and r["duck"] else "—"
        print(f"  {r['name']:<40} {pg_str:>10} {duck_str:>10} {ratio_str:>8}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lesson 3: DuckDB vs Postgres benchmark")
    parser.add_argument("--pg-only", action="store_true", help="Only run Postgres")
    parser.add_argument("--duck-only", action="store_true", help="Only run DuckDB")
    args = parser.parse_args()
    main(pg_only=args.pg_only, duck_only=args.duck_only)
