"""Probe and explore the current state of both Postgres and DuckDB data.

Shows row counts, date ranges, and sample data for all tables. Use this to
understand what's currently loaded before running pipelines or experiments.

Usage:
    python src/probe_data.py                    # show current state
    python src/probe_data.py --reset            # reset and regenerate seed data
"""

import argparse
import subprocess
import sys

import psycopg

from config import PG_DSN, connect_target


def probe_postgres() -> None:
    """Show Postgres source data state."""
    print("\n" + "=" * 60)
    print("POSTGRES (OLTP SOURCE)")
    print("=" * 60)
    
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        # Orders
        cur.execute("SELECT COUNT(*) FROM orders")
        orders_count = cur.fetchone()[0]
        cur.execute("SELECT MIN(created_at), MAX(created_at) FROM orders")
        orders_range = cur.fetchone()
        cur.execute("SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status")
        status_counts = cur.fetchall()
        
        print(f"\norders: {orders_count:,} rows")
        print(f"  date range: {orders_range[0]} → {orders_range[1]}")
        print("  by status:")
        for status, count in status_counts:
            print(f"    {status:<12} {count:,}")
        
        # Customers
        cur.execute("SELECT COUNT(*) FROM customers")
        customers_count = cur.fetchone()[0]
        cur.execute("SELECT city, COUNT(*) FROM customers GROUP BY city ORDER BY city")
        city_counts = cur.fetchall()
        
        print(f"\ncustomers: {customers_count:,} rows")
        print("  by city:")
        for city, count in city_counts:
            print(f"    {city:<12} {count:,}")
        
        # Sample orders
        cur.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 3")
        print("\nSample orders (latest 3):")
        for row in cur.fetchall():
            print(f"  id={row[0]} customer_id={row[1]} amount=${row[2]:.2f} status={row[3]} {row[4]}")


def probe_duckdb() -> None:
    """Show DuckDB target data state."""
    print("\n" + "=" * 60)
    print("DUCKDB (ANALYTICAL TARGET)")
    print("=" * 60)
    
    con = connect_target()
    
    # Get all tables
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name").fetchall()
    
    for (table_name,) in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\n{table_name}: {count:,} rows")
        
        # Try to get date range if there's a date column
        try:
            if table_name == "daily_revenue" or table_name == "daily_revenue_keyed":
                date_range = con.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}").fetchone()
                if date_range[0]:
                    print(f"  date range: {date_range[0]} → {date_range[1]}")
                    status_counts = con.execute(f"SELECT status, COUNT(*) FROM {table_name} GROUP BY status ORDER BY status").fetchall()
                    print("  by status:")
                    for status, cnt in status_counts:
                        print(f"    {status:<12} {cnt:,}")
            elif table_name == "customers_dim":
                date_range = con.execute(f"SELECT MIN(valid_from), MAX(valid_from) FROM {table_name}").fetchone()
                if date_range[0]:
                    print(f"  valid_from range: {date_range[0]} → {date_range[1]}")
                    current_count = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE is_current").fetchone()[0]
                    print(f"  current versions: {current_count:,}")
            elif table_name == "pipeline_metadata":
                date_range = con.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}").fetchone()
                if date_range[0]:
                    print(f"  date range: {date_range[0]} → {date_range[1]}")
        except Exception:
            pass
        
        # Sample rows
        try:
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 2").fetchall()
            if sample:
                print("  sample rows:")
                for row in sample:
                    print(f"    {row}")
        except Exception:
            pass
    
    con.close()


def reset_data() -> None:
    """Reset both databases by calling seed_data.py and dropping DuckDB file."""
    print("\n" + "=" * 60)
    print("RESETTING DATA")
    print("=" * 60)
    
    # Drop DuckDB file
    from config import DUCKDB_PATH
    if DUCKDB_PATH.exists():
        print(f"\nDeleting DuckDB file: {DUCKDB_PATH}")
        DUCKDB_PATH.unlink()
        print("  Done.")
    else:
        print(f"\nDuckDB file does not exist: {DUCKDB_PATH}")
    
    # Re-seed Postgres
    print("\nRe-seeding Postgres via seed_data.py...")
    result = subprocess.run(
        [sys.executable, "src/seed_data.py"],
        cwd=str(DUCKDB_PATH.parent.parent),
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print("ERROR:", result.stderr)
        sys.exit(1)
    
    print("\nReset complete.")


def main(reset: bool = False) -> None:
    if reset:
        reset_data()
    else:
        probe_postgres()
        probe_duckdb()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Probe or reset data in Postgres and DuckDB")
    p.add_argument("--reset", action="store_true", help="reset data: drop DuckDB file and re-seed Postgres")
    args = p.parse_args()
    main(args.reset)
