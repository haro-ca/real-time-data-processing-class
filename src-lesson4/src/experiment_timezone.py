"""Experiment — the daily partition boundary moves with the session timezone.

The pipeline slices a day with `WHERE created_at::date = %s`. But created_at is a
TIMESTAMPTZ, and casting it to ::date resolves in the *session* timezone. So the
exact same rows fall on different calendar days under UTC vs local time — orders
near midnight cross the boundary. A backfill run in one zone and re-run in
another silently disagrees about what "2024-01-15" contains.

This counts one date's orders under a few zones so you can see the spread. Fix in
practice: pin a timezone (SET TimeZone) or filter on an explicit expression like
(created_at AT TIME ZONE 'UTC')::date, so partitions are reproducible.

Usage:
    python src/experiment_timezone.py 2024-01-15
    python src/experiment_timezone.py 2024-01-15 --zones UTC Asia/Tokyo
"""

import argparse
from datetime import date

import psycopg

from config import PG_DSN

ZONES = ["UTC", "America/Los_Angeles", "Asia/Tokyo"]


def count_for(cur, target_date: date, zone: str) -> int:
    cur.execute("SET TimeZone = %s", (zone,))
    cur.execute(
        "SELECT COUNT(*) FROM orders WHERE created_at::date = %s", (target_date,)
    )
    return cur.fetchone()[0]


def main(target_date: date, zones: list[str]) -> None:
    print(f"orders WHERE created_at::date = {target_date}, by session timezone:\n")
    counts: dict[str, int] = {}
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        for z in zones:
            counts[z] = count_for(cur, target_date, z)
            print(f"  {z:<22} {counts[z]:>10,}")

    spread = max(counts.values()) - min(counts.values())
    print()
    if spread:
        print(f"  SAME DATA, but {spread:,} rows move across the day boundary by zone.")
        print("  -> pin a timezone (or cast via AT TIME ZONE) for reproducible partitions.")
    else:
        print("  No near-midnight rows shifted for this date — try another, or reseed.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Show timezone-dependent partition boundaries")
    p.add_argument("target_date", nargs="?", default="2024-01-15")
    p.add_argument("--zones", nargs="*", default=ZONES, help="session timezones to compare")
    args = p.parse_args()
    main(date.fromisoformat(args.target_date), args.zones)
