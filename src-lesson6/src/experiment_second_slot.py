"""The hook: what the SECOND consumer costs when the OLTP source is your buffer.

Runs against the LESSON 5 Postgres (it must be up: cd ../src-lesson5 && docker
compose up -d). Two new teams each want the change stream, so each gets its own
replication slot. One team's consumer keeps pace (we advance its slot); the
other team's consumer is "down for maintenance" — i.e. the realistic case.

Watch pg_replication_slots while a write workload runs:
  - the consumed slot stays near zero retained WAL
  - the stalled slot pins WAL on the SOURCE, growing without bound
  - every additional reader = another slot = another independent time bomb,
    and every slot decodes the WAL separately (CPU on the source, again)

The fix is not "monitor harder." It's moving the buffer OFF the source: a log
in the middle that retains by time and lets readers track their own positions.

Usage:
    python src/experiment_second_slot.py                 # ~20s, then cleans up
    python src/experiment_second_slot.py --seconds 30
"""

import argparse
import time

import psycopg

from config import PG_DSN

SLOTS = ["team_fraud", "team_search"]      # the two new teams
SCRATCH = "l6_hook_traffic"                # our own table: don't touch L5's data
REPORT_EVERY = 20_000                      # rows between status lines (and checkpoints)

LAG_SQL = """
    SELECT slot_name,
           pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
           active
    FROM pg_replication_slots
    WHERE slot_name = ANY(%s)
    ORDER BY slot_name
"""


def run(seconds: int) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        print("two new teams want the CDC stream -> two new slots on the SOURCE:\n")
        for slot in SLOTS:
            pg.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'wal2json')"
                " WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                (slot, slot),
            )
            print(f"  created slot {slot}")
        pg.execute(f"CREATE TABLE IF NOT EXISTS {SCRATCH} (id BIGINT, payload TEXT)")

        print(f"\nwrite workload for {seconds}s — {SLOTS[0]}'s consumer keeps pace,"
              f" {SLOTS[1]}'s is down:\n")
        def keep_pace_and_reclaim() -> None:
            """team_fraud's consumer keeps pace, and we RECLAIM its WAL.

            Draining a slot (get_changes) advances confirmed_flush_lsn, but the
            restart_lsn — the point that actually pins WAL on disk — only moves
            forward at a CHECKPOINT. So: drain, checkpoint, drain again (the
            second drain decodes the checkpoint's running-xact record and lets
            restart_lsn jump to now). team_search never reads, so the checkpoint
            cannot free its WAL: that's the contrast we want on screen.
            """
            for _ in range(2):
                pg.execute(
                    "SELECT count(*) FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                    (SLOTS[0],),
                )
                pg.execute("CHECKPOINT")

        t0 = time.time()
        i = 0
        while time.time() - t0 < seconds:
            # the workload: bulk-ish inserts so the WAL actually moves
            pg.execute(
                # cast the bounds: psycopg adapts small ints to smallint, and
                # generate_series(smallint, smallint) is ambiguous in Postgres.
                f"INSERT INTO {SCRATCH} SELECT g, repeat('x', 200) "
                f"FROM generate_series(%s::bigint, %s::bigint) g", (i, i + 999),
            )
            i += 1000
            if i % REPORT_EVERY == 0:
                keep_pace_and_reclaim()
                rows = pg.execute(LAG_SQL, (SLOTS,)).fetchall()
                ts = time.strftime("%H:%M:%S")
                stat = "   ".join(f"{name}: {retained} retained" for name, retained, _ in rows)
                print(f"  [{ts}] rows={i:>7,}   {stat}")

        keep_pace_and_reclaim()
        print("\nfinal state:")
        retained = {name: r for name, r, _ in pg.execute(LAG_SQL, (SLOTS,)).fetchall()}
        print(f"  {SLOTS[0]:<14} retained WAL: {retained[SLOTS[0]]:<11}"
              f" <- consumer kept pace; freed at the last checkpoint")
        print(f"  {SLOTS[1]:<14} retained WAL: {retained[SLOTS[1]]:<11}"
              f" <- nobody reading; this only ever GROWS")

        print("\ncleanup (dropping slots + scratch table)...")
        for slot in SLOTS:
            pg.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
        pg.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
        print("done. The source survived because we remembered to clean up. "
              "Production forgets.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="The cost of reader #2 on the source")
    p.add_argument("--seconds", type=int, default=20)
    args = p.parse_args()
    run(args.seconds)
