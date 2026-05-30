"""Experiment C: Data ordering impact on zone maps.

Creates two copies of the trips data in DuckDB:
  - trips_by_time: sorted by pickup_datetime (zone maps effective)
  - trips_shuffled: random order (zone maps useless)

Then runs the SAME date-filtered query across multiple filter widths to show
HOW zone maps' effectiveness scales with selectivity. The tighter the filter,
the more dramatically sorting wins.

Filter dates target a window inside the dataset's date range — adjust the
WINDOWS list if you change which months you've downloaded.

Usage:
    uv run python experiment_ordering.py
"""

import time
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent / "data"
PARQUET_GLOB = str(DATA_DIR / "yellow_tripdata_*.parquet")
DB_PATH = str(DATA_DIR / "ordering_experiment.duckdb")

# Filter windows of increasing width — selective filter wins big from zone maps,
# broad filter gets little benefit (must read most row groups either way). Dates
# target the workshop default (Q1 2025 download).
WINDOWS = [
    ("1 day",    "'2025-02-15'", "'2025-02-16'"),
    ("1 week",   "'2025-02-15'", "'2025-02-22'"),
    ("1 month",  "'2025-02-01'", "'2025-03-01'"),
    ("3 months", "'2025-01-01'", "'2025-04-01'"),
]


def setup(con: duckdb.DuckDBPyConnection) -> None:
    """Create sorted and shuffled tables if they don't exist."""
    tables = {r[0] for r in con.sql("SHOW TABLES").fetchall()}

    if "trips_by_time" not in tables:
        print("  Creating trips_by_time (sorted by pickup_datetime)...")
        t0 = time.monotonic()
        con.sql(f"""
            CREATE TABLE trips_by_time AS
            SELECT * FROM read_parquet('{PARQUET_GLOB}', union_by_name=true)
            ORDER BY tpep_pickup_datetime
        """)
        print(f"    Done in {time.monotonic() - t0:.1f}s")
    else:
        print("  trips_by_time already exists")

    if "trips_shuffled" not in tables:
        print("  Creating trips_shuffled (random order)...")
        t0 = time.monotonic()
        con.sql(f"""
            CREATE TABLE trips_shuffled AS
            SELECT * FROM read_parquet('{PARQUET_GLOB}', union_by_name=true)
            ORDER BY RANDOM()
        """)
        print(f"    Done in {time.monotonic() - t0:.1f}s")
    else:
        print("  trips_shuffled already exists")


def bench(con: duckdb.DuckDBPyConnection, table: str, lo: str, hi: str) -> float:
    """Median of 3 runs in ms."""
    sql = f"""
        SELECT DATE_TRUNC('hour', tpep_pickup_datetime) AS h, AVG(fare_amount)
        FROM {table}
        WHERE tpep_pickup_datetime >= {lo} AND tpep_pickup_datetime < {hi}
        GROUP BY h ORDER BY h
    """
    times = []
    for _ in range(3):
        t0 = time.monotonic()
        con.sql(sql).fetchall()
        times.append(time.monotonic() - t0)
    times.sort()
    return times[1] * 1000  # median, ms


def main() -> None:
    print("Experiment C — Data Ordering Impact on Zone Maps")
    print("=" * 70)

    con = duckdb.connect(DB_PATH)
    setup(con)
    print()

    # Warm up the file pages once so the first window isn't penalized
    con.sql("SELECT COUNT(*) FROM trips_by_time").fetchone()
    con.sql("SELECT COUNT(*) FROM trips_shuffled").fetchone()

    print(f"  {'Filter window':<12} {'sorted (ms)':>14} {'shuffled (ms)':>16} {'ratio':>10}")
    print(f"  {'-'*12} {'-'*14} {'-'*16} {'-'*10}")
    for label, lo, hi in WINDOWS:
        s = bench(con, "trips_by_time", lo, hi)
        sh = bench(con, "trips_shuffled", lo, hi)
        print(f"  {label:<12} {s:14.2f} {sh:16.2f} {sh/s:9.1f}×")
    print()
    print("  Lesson: zone maps eliminate row groups whose [min, max] range")
    print("  doesn't overlap the filter. The tighter the filter, the more")
    print("  row groups get eliminated. Broad scans = no benefit. Selective")
    print("  filters = orders-of-magnitude speedup.")
    print()
    print("  Inspect with: EXPLAIN ANALYZE SELECT ... FROM trips_by_time WHERE ...")

    con.close()


if __name__ == "__main__":
    main()
