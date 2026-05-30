"""Load NYC Taxi Parquet data into Postgres, streaming via DuckDB's postgres extension.

The DuckDB postgres extension issues a server-side COPY under the hood and streams
batches directly from the parquet reader into Postgres — no intermediate CSV file
on disk. This matters on laptops with tight disk budgets (a CSV intermediate for
128M rows is ~13 GB).

Index is dropped before insert and recreated after — building incrementally during
a 128M-row bulk load is dramatically slower than building once at the end.

Usage:
    uv run python load_postgres.py [--limit 1000000]
"""

import argparse
import os
import time
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent / "data"
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_CONN_STR = f"host={PG_HOST} port={PG_PORT} user=bench password=bench dbname=bench"


def load(limit: int | None = None) -> None:
    parquet_glob = str(DATA_DIR / "yellow_tripdata_*.parquet")

    print("Loading NYC Taxi data into Postgres...")
    print(f"  Source: {parquet_glob}")

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres")
    con.sql(f"ATTACH '{PG_CONN_STR}' AS pg (TYPE postgres)")

    total = con.sql(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}', union_by_name=true)"
    ).fetchone()[0]
    print(f"  Total rows available: {total:,}")

    rows_to_load = limit or total
    print(f"  Loading: {rows_to_load:,} rows")
    print()

    print("  Resetting target table (TRUNCATE + drop index)...")
    con.sql("CALL postgres_execute('pg', 'DROP INDEX IF EXISTS idx_trips_pickup')")
    con.sql("CALL postgres_execute('pg', 'TRUNCATE trips')")

    t0 = time.monotonic()
    print("  Streaming parquet → Postgres COPY...")
    # union_by_name handles: (a) airport_fee vs Airport_fee case drift between 2023
    # and 2024+, and (b) cbd_congestion_fee being absent from years before 2025.
    con.sql(f"""
        INSERT INTO pg.trips
        SELECT
            CAST(VendorID AS INTEGER)              AS vendor_id,
            tpep_pickup_datetime                   AS pickup_datetime,
            tpep_dropoff_datetime                  AS dropoff_datetime,
            CAST(passenger_count AS INTEGER)       AS passenger_count,
            trip_distance,
            CAST(RatecodeID AS INTEGER)            AS rate_code_id,
            store_and_fwd_flag,
            CAST(PULocationID AS INTEGER)          AS pickup_location_id,
            CAST(DOLocationID AS INTEGER)          AS dropoff_location_id,
            CAST(payment_type AS INTEGER)          AS payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee
        FROM read_parquet('{parquet_glob}', union_by_name=true)
        LIMIT {rows_to_load}
    """)
    copy_time = time.monotonic() - t0
    print(f"  INSERT (streamed COPY): {copy_time:.1f}s")

    t1 = time.monotonic()
    print("  Building index on pickup_datetime...")
    con.sql(
        "CALL postgres_execute('pg', 'CREATE INDEX idx_trips_pickup ON trips (pickup_datetime)')"
    )
    index_time = time.monotonic() - t1
    print(f"  CREATE INDEX:           {index_time:.1f}s")

    t2 = time.monotonic()
    print("  Running ANALYZE...")
    con.sql("CALL postgres_execute('pg', 'ANALYZE trips')")
    analyze_time = time.monotonic() - t2
    print(f"  ANALYZE:                {analyze_time:.1f}s")

    con.close()

    total_time = time.monotonic() - t0
    print()
    print(f"  Total: {total_time:.1f}s for {rows_to_load:,} rows "
          f"({rows_to_load / total_time:,.0f} rows/sec)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Parquet into Postgres")
    parser.add_argument("--limit", "-l", type=int, default=None,
                        help="Limit rows to load (default: all)")
    args = parser.parse_args()
    load(args.limit)
