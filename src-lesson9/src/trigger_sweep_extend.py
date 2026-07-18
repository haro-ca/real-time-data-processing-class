"""Extend trigger_sweep.py's data with sub-second trigger points.

trigger_sweep.py measured [1, 2, 5, 10]s and found Spark and Flink nearly
tied at trigger=1s. This answers the natural follow-up (slide 13's "your
turn" exercise): does Spark's floor keep falling toward Flink's below 1s,
or does it hit a wall first? Reuses trigger_sweep's own run_round()/plot()
so the methodology (topic isolation, warmup/drain, reset between rounds) is
identical — this only adds two new data points, it doesn't re-run the four
we already trust.

Usage:
    uv run python src/trigger_sweep_extend.py
"""

import json
import sys

from config import DATA_DIR, banner
from trigger_sweep import plot, run_round

NEW_TRIGGERS = [0.5, 0.25]


def main():
    banner(
        "Trigger sweep extension: sub-second triggers",
        f"new points: {NEW_TRIGGERS}",
        "merging with existing trigger_sweep.json [1, 2, 5, 10]",
    )

    existing = json.loads((DATA_DIR / "trigger_sweep.json").read_text())
    new_results = [run_round(t) for t in NEW_TRIGGERS]

    combined = sorted(existing + new_results, key=lambda r: r["trigger_seconds"])

    out_path = DATA_DIR / "trigger_sweep.json"
    out_path.write_text(json.dumps(combined, indent=2) + "\n")
    print(f"\ncombined results saved to {out_path}")
    print(json.dumps(combined, indent=2))

    plot(combined, DATA_DIR / "trigger_sweep.png")


if __name__ == "__main__":
    main()
