"""Watch replication-slot lag — the single most important CDC operational signal.

lag_bytes = how far the slot's confirmed position trails the current WAL.
  flat / small  -> the consumer is keeping pace.
  growing       -> the consumer is falling behind (or stopped — see retained).
  retained      -> WAL the source MUST keep until the slot confirms it. If this
                   grows unbounded (an abandoned slot), it eventually fills the disk.

This is the raw version of what Debezium exposes as a JMX metric and Kafka Connect
calls "consumer lag". Alert on it in production; an unmonitored slot is a time bomb.

Usage:
    python src/watch_lag.py                 # poll every 2s, forever (Ctrl-C)
    python src/watch_lag.py --interval 1 --iters 10
"""

import argparse
import time

import psycopg

from config import PG_DSN

LAG_SQL = """
    SELECT slot_name,
           active,
           pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))        AS retained,
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)                AS lag_bytes
    FROM pg_replication_slots
    ORDER BY slot_name
"""


def watch(interval: float, iters: int | None) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        i = 0
        while iters is None or i < iters:
            rows = pg.execute(LAG_SQL).fetchall()
            ts = time.strftime("%H:%M:%S")
            if not rows:
                print(f"[{ts}] no replication slots. Run setup_cdc.py first.")
            else:
                print(f"[{ts}] {'slot':<16}{'active':<8}{'retained':<12}{'lag_bytes':>12}")
                for name, active, retained, lag in rows:
                    flag = "" if active else "  <- no consumer!"
                    print(f"         {name:<16}{str(active):<8}{retained:<12}{lag:>12,}{flag}")
            i += 1
            if iters is None or i < iters:
                time.sleep(interval)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Monitor replication-slot lag")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--iters", type=int, default=None, help="stop after N samples")
    args = p.parse_args()
    try:
        watch(args.interval, args.iters)
    except KeyboardInterrupt:
        print("\nstopped.")
