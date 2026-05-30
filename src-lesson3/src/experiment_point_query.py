"""Experiment A: Point queries — DuckDB's weakness.

Shows that for single-row lookups, Postgres with a B-tree index
crushes DuckDB which must scan zone maps and decompress segments.

Usage:
    python experiment_point_query.py [--iterations 100]
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

# A timestamp inside the workshop-default Q1 2025 window. Whether the row exists
# is irrelevant for the benchmark — both engines pay the lookup cost either way
# (B-tree probe for Postgres, full segment scan for DuckDB).
LOOKUP_VALUE = "2025-02-15 14:30:00"


def bench_postgres(iterations: int) -> float:
    """Point lookup with B-tree index."""
    sql = "SELECT * FROM trips WHERE pickup_datetime = %s LIMIT 1"
    latencies = []

    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for _ in range(iterations):
                t0 = time.monotonic()
                cur.execute(sql, (LOOKUP_VALUE,))
                cur.fetchall()
                latencies.append(time.monotonic() - t0)

    p50_ms = sorted(latencies)[len(latencies) // 2] * 1000
    return p50_ms


def bench_duckdb(iterations: int) -> float:
    """Point lookup — no index, must scan zone maps."""
    sql = f"""
        SELECT * FROM '{PARQUET_GLOB}'
        WHERE tpep_pickup_datetime = '{LOOKUP_VALUE}'
        LIMIT 1
    """
    latencies = []

    con = duckdb.connect()
    for _ in range(iterations):
        t0 = time.monotonic()
        con.sql(sql).fetchall()
        latencies.append(time.monotonic() - t0)
    con.close()

    p50_ms = sorted(latencies)[len(latencies) // 2] * 1000
    return p50_ms


def main(iterations: int) -> None:
    print("Experiment A — Point Query: Postgres vs DuckDB")
    print(f"  Lookup: pickup_datetime = '{LOOKUP_VALUE}'")
    print(f"  Iterations: {iterations}")
    print("-" * 50)

    pg_p50 = bench_postgres(iterations)
    print(f"  Postgres (B-tree index):  p50 = {pg_p50:.2f} ms")

    duck_p50 = bench_duckdb(iterations)
    print(f"  DuckDB (no index):        p50 = {duck_p50:.2f} ms")

    print()
    if duck_p50 > pg_p50:
        print(f"  Postgres wins: {duck_p50 / pg_p50:.0f}× faster for point lookups")
    else:
        print(f"  DuckDB wins (unexpected): {pg_p50 / duck_p50:.1f}×")

    print()
    print("  Lesson: column stores are NOT a replacement for row stores.")
    print("  They serve different access patterns.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment A: Point queries")
    parser.add_argument("--iterations", "-n", type=int, default=50,
                        help="Number of point lookups to run (default: 50)")
    args = parser.parse_args()
    main(args.iterations)
