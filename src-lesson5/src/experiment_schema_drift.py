"""Break 03: a column appears mid-stream (slide "Break 03 · live").

Two acts:

  Act 1 — the silent lie. ALTER TABLE orders ADD COLUMN notes on the SOURCE,
  update a row, and run the consumer with --ignore-drift. It applies the event
  and silently drops the new column: no error, and the mirror disagrees with the
  source. The exact failure mode polling had, except this time we wrote it.

  Act 2 — fail loud. Update another row and run the consumer in its default
  strict mode. It detects the unknown column, refuses to apply, and exits with a
  clear message. Nothing is lost: the slot was never advanced, so after you fix
  the mirror the event simply replays. Confirm-after-apply is what makes "crash"
  a safe answer.

By default the script restores the source (DROP COLUMN notes) and drains the
slot at the end, so the rest of the lesson keeps working. Pass --keep to leave
the drifted state in place (the take-home asks you to handle it yourself).

Usage:
    python src/experiment_schema_drift.py           # both acts, then clean up
    python src/experiment_schema_drift.py --keep    # leave the drift in place
"""

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb
import psycopg

from config import DUCKDB_PATH, PG_DSN

SRC = Path(__file__).parent


def run_consumer(ignore_drift: bool) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(SRC / "cdc_consumer.py"), "--once"]
    if ignore_drift:
        cmd.append("--ignore-drift")
    return subprocess.run(cmd, capture_output=True, text=True)


def mirror_columns() -> set[str]:
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='orders'"
    ).fetchall()}
    con.close()
    return cols


def run(keep: bool) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        print("baseline: draining the slot so we start in sync...")
        run_consumer(ignore_drift=False)

        # ids are sequence-assigned and don't start at 1 — grab two real rows
        id_a, id_b = (r[0] for r in pg.execute(
            "SELECT id FROM orders ORDER BY id LIMIT 2").fetchall())

        print("\n--- ACT 1 · the silent lie (--ignore-drift) " + "-" * 24)
        pg.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS notes TEXT")
        pg.execute("UPDATE orders SET notes = 'gift wrap, fragile' WHERE id = %s", (id_a,))
        print("source:  ALTER TABLE orders ADD COLUMN notes TEXT")
        print(f"source:  UPDATE orders SET notes = 'gift wrap, fragile' WHERE id = {id_a}")

        r = run_consumer(ignore_drift=True)
        src_notes = pg.execute(
            "SELECT notes FROM orders WHERE id = %s", (id_a,)).fetchone()[0]
        has_notes = "notes" in mirror_columns()
        print(f"\nconsumer --ignore-drift exited {r.returncode} (no error raised)")
        print(f"  source  row {id_a}  notes = {src_notes!r}")
        print(f"  mirror  row {id_a}  notes = {'<column does not exist>' if not has_notes else 'present'}")
        print("  THE MIRROR LIES. Nothing crashed. Nobody will notice until finance does.")

        print("\n--- ACT 2 · fail loud (default strict mode) " + "-" * 24)
        pg.execute("UPDATE orders SET notes = 'expedite' WHERE id = %s", (id_b,))
        print(f"source:  UPDATE orders SET notes = 'expedite' WHERE id = {id_b}")
        r = run_consumer(ignore_drift=False)
        print(f"\nconsumer (strict) exited {r.returncode}:")
        for line in r.stderr.strip().splitlines():
            print(f"  {line}")

        if keep:
            print("\n--keep: source still has 'notes', slot still holds the event.")
            print("Your move (the take-home): ALTER the mirror, or keep failing loud.")
            return

        print("\ncleanup: restoring the source and draining the slot...")
        pg.execute("ALTER TABLE orders DROP COLUMN IF EXISTS notes")
        run_consumer(ignore_drift=True)  # consume the drifted events still in the slot
        print("  source restored (notes dropped), slot drained, mirror converged.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Schema drift: silent lie vs. fail loud")
    p.add_argument("--keep", action="store_true",
                   help="leave the drifted column and pending event in place")
    args = p.parse_args()
    run(args.keep)
