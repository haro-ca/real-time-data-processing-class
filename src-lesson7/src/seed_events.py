"""Seed the `orders-cdc` topic with order events — most on time, 5% LATE.

This is the test population for the whole lesson. Event times march forward from
the anchor (config.base_time(), today 12:00) across --span minutes. A fraction
(--late-fraction, default 5%) are stamped in the PAST relative to the marching
cursor: their created_at is earlier than the events around them in the log. They
are the mobile-client / retry / CDC-lag stragglers every real pipeline carries,
and they are what the watermark will or won't drop.

Lateness is drawn from an exponential (mean --late-mean minutes, clamped 1–45),
so it has a real tail past 10 minutes — that tail is what a 10-minute watermark
still catches. A uniform 1–10 would drop exactly nothing at a 10-min watermark.

This is a plain confluent-kafka producer — no Spark. produce() only ENQUEUES;
poll() pumps callbacks, flush() drains before exit (the L6 gotcha).

IDEMPOTENT: by default the topic is deleted and recreated before seeding, so a
re-run leaves exactly --count events over the same event-time window — no
duplicate pile-up. Pass --append to add to an existing topic instead.

Usage:
    python src/seed_events.py                       # 10k orders, 5% late (resets first)
    python src/seed_events.py --count 5000 --span 30
    python src/seed_events.py --late-fraction 0.0   # a clean, no-drops baseline
    python src/seed_events.py --append              # add more, don't reset
"""

import argparse
import json
import random
import time
from collections import Counter
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, TOPIC, banner, base_time, iso, lesson

STATUSES = ["created", "paid", "shipped", "delivered"]

# 50 customers, zipf-ish skew (a few whales) — gives stream_to_kafka's
# per-customer window something lopsided to chew on, and makes per-partition
# event-time progress uneven (the "watermark is global" gotcha, for real).
CUSTOMERS = list(range(50))
WEIGHTS = [1 / (c + 1) for c in CUSTOMERS]


def reset_topic(partitions: int, append: bool, count: int) -> None:
    """Idempotency, the easy way: delete the topic and recreate it before seeding,
    so every run yields exactly `count` events over the same event-time window —
    re-running can't pile up duplicates. --append skips the delete to add more."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    exists = TOPIC in admin.list_topics(timeout=10).topics

    if append:
        print(f"  topic: --append → keeping '{TOPIC}'"
              + (f", adding {count:,} more events" if exists else " (creating it)"))
    elif exists:                                  # reset → delete, then wait until gone
        print(f"  topic: deleting old '{TOPIC}' for a clean, idempotent reseed")
        for _, f in admin.delete_topics([TOPIC], operation_timeout=30).items():
            try:
                f.result(timeout=30)
            except Exception as e:
                if "UNKNOWN_TOPIC_OR_PART" not in str(e):
                    raise
        for _ in range(30):                       # deletion is async — wait it out
            if TOPIC not in admin.list_topics(timeout=10).topics:
                break
            time.sleep(1)

    if append and exists:
        return
    for _ in range(30):                           # create, retrying while the old one clears
        f = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions,
                                          replication_factor=1)])[TOPIC]
        try:
            f.result(timeout=30)
            print(f"  topic: created '{TOPIC}' fresh ({partitions} partitions, RF=1)")
            return
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  topic: '{TOPIC}' already present ({partitions} partitions)")
                return
            if "delet" in str(e).lower():          # still being deleted — wait and retry
                time.sleep(1)
                continue
            raise


def run(count: int, span_min: float, late_fraction: float, late_mean: float,
        partitions: int, append: bool) -> None:
    base = base_time()
    mode = ("APPEND — adds to existing data (not idempotent)" if append
            else "idempotent — topic is reset first, so a re-run gives the same world")
    banner("seed_events · building the lesson's world",
           f"writes a batch of {count:,} orders to Kafka topic '{TOPIC}'",
           f"event times march from {iso(base)} across {span_min:g} min: the 'business",
           "  clock' (created_at) that every windowed aggregate groups by",
           f"~{late_fraction:.0%} are stamped in the PAST (exp mean {late_mean:g}m, tail to ~45m) — the late",
           "  stragglers the watermark must keep or drop (the lesson's test set)",
           mode,
           "every L7 demo then replays THIS static topic from earliest")

    reset_topic(partitions, append, count)
    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})

    step_s = (span_min * 60.0) / max(1, count)   # cursor advance per event
    delivered = 0
    errors = 0
    late_count = 0
    lateness_hist = Counter()   # minutes-late bucket → n
    sample_orders = []          # capture first 10 for inspection

    def on_delivery(err, _msg):
        nonlocal delivered, errors
        if err:
            errors += 1
        else:
            delivered += 1

    print(f"\n  producing {count:,} orders ({late_fraction:.0%} late)…")

    for i in range(count):
        cursor = base.timestamp() + i * step_s
        is_late = random.random() < late_fraction
        if is_late:
            lateness_min = min(45.0, max(1.0, random.expovariate(1.0 / late_mean)))
            event_ts = cursor - lateness_min * 60.0
            late_count += 1
            lateness_hist[int(lateness_min)] += 1
        else:
            event_ts = cursor

        order = {
            "order_id": i,
            "customer_id": random.choices(CUSTOMERS, WEIGHTS)[0],
            "amount": round(10 + random.random() * 140, 2),
            "status": random.choice(STATUSES),
            "created_at": iso(datetime.fromtimestamp(event_ts)),
        }
        if len(sample_orders) < 10:
            sample_orders.append(order)
        producer.produce(TOPIC,
                         key=str(order["customer_id"]).encode(),
                         value=json.dumps(order).encode(),
                         callback=on_delivery)
        producer.poll(0)                       # pump callbacks — not optional

    producer.flush(60)                         # drain before exit — not optional
    print(f"\ndelivered {delivered:,} ({errors} errors), of which {late_count:,} late")
    print("\nSample 10 orders:")
    for i, order in enumerate(sample_orders, 1):
        print(f"  {i}. {json.dumps(order, indent=2)}")
    if lateness_hist:
        print("lateness (minutes → count), the watermark sweep will eat the left tail:")
        for m in sorted(lateness_hist):
            n = lateness_hist[m]
            print(f"  {m:>2}–{m+1:<2}m  {'#' * max(1, n // 3)}  {n}")

    pct = late_count / max(1, delivered)
    lesson(
        f"{late_count:,} of {delivered:,} orders ({pct:.1%}) carry an event time in the PAST —",
        "  they sit out of order in the log, exactly like L6's shuffled topic.",
        "Event time (created_at) is NOT arrival order. Today's whole job is deciding",
        "  which of these stragglers still count: that decision is the watermark.",
        "Next → stream_revenue.py turns this static topic into windowed revenue.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Seed orders-cdc with late stragglers")
    p.add_argument("--count", type=int, default=10_000)
    p.add_argument("--span", type=float, default=60.0, help="event-time span, minutes")
    p.add_argument("--late-fraction", type=float, default=0.05)
    p.add_argument("--late-mean", type=float, default=7.0,
                   help="mean lateness in minutes (exponential, clamped 1–45)")
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--append", action="store_true",
                   help="add to the existing topic instead of resetting it (not idempotent)")
    args = p.parse_args()
    run(args.count, args.span, args.late_fraction, args.late_mean, args.partitions,
        args.append)
