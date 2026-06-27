"""The counter that tells on the watermark — run this in a SECOND terminal.

Slide 24 calls it "reads query.lastProgress". The honest detail: lastProgress is
a handle inside the pipeline's OWN driver process, so a separate process can't read
it directly. Instead stream_revenue.py polls lastProgress and appends each batch to
data/progress.jsonl; this script tails that file. Same numbers, one process hop.

The headline is numRowsDroppedByWatermark. Every block of this course ends at one
operational number — TPS (L1), slot lag (L5), consumer lag (L6) — and this is L7's.
Zero means your lateness allowance is generous enough. Nonzero means you are
shipping wrong totals knowingly: a business decision the moment you can see it.

Usage:
    python src/watch_progress.py        # tail forever (Ctrl-C to stop)
    python src/watch_progress.py --from-start   # replay the whole file first
"""

import argparse
import json
import time

from config import PROGRESS_FILE, banner, fmt_watermark, lesson


def fmt(s: dict, dropped_total: int) -> str:
    return (
        f"batch {s['batchId']:>4}   input {s['inputRowsPerSecond']:>7}/s   "
        f"processed {s['processedRowsPerSecond']:>7}/s   wm {fmt_watermark(s['watermark'])}\n"
        f"  state:  numRowsTotal              {s['numRowsTotal']:>7,}   "
        f"(open windows being remembered)\n"
        f"          numRowsUpdated            {s['numRowsUpdated']:>7,}\n"
        f"          numRowsDroppedByWatermark {s['numRowsDroppedByWatermark']:>7,}"
        + (f"   ← {dropped_total:,} total, the revenue you can't see\n"
           if dropped_total else "\n"))


def run(from_start: bool) -> None:
    banner("watch_progress · the operational metric (terminal 2)",
           f"tails {PROGRESS_FILE.name}, which stream_revenue writes one line per micro-batch",
           "surfaces numRowsDroppedByWatermark — L7's one operational number, the lineage",
           "  after L1 TPS · L5 slot lag · L6 consumer lag",
           "zero = your lateness allowance is generous enough;",
           "nonzero = you're shipping wrong totals KNOWINGLY (a decision, not a bug)")
    print(f"\ntailing {PROGRESS_FILE}  (start a pipeline in terminal 1)\n")
    while not PROGRESS_FILE.exists():
        time.sleep(0.5)

    dropped_total = 0
    with PROGRESS_FILE.open() as fh:
        if not from_start:
            fh.seek(0, 2)                       # jump to end: only new batches
        try:
            while True:
                line = fh.readline()
                if not line:
                    time.sleep(0.3)
                    continue
                line = line.strip()
                if not line:
                    continue
                s = json.loads(line)
                dropped_total += s["numRowsDroppedByWatermark"]
                print(fmt(s, dropped_total))
        except KeyboardInterrupt:
            lesson(
                f"total dropped this session: {dropped_total:,}",
                "this counter is a BINARY tripwire (zero vs nonzero), not a precise loss tally:",
                "  it counts dropped aggregation GROUPS, not events — 50 late orders in one",
                "  window register as +1. Quantify the lost dollars with the batch audit.",
                "Use this to KNOW you're dropping; use the batch pipeline to say how much.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Tail streaming progress; surface drops")
    p.add_argument("--from-start", action="store_true",
                   help="replay the whole progress file before following")
    args = p.parse_args()
    run(args.from_start)
