"""Produce order events to the `orders` topic, keyed by customer_id.

The producer config IS the lesson:
    acks=all              wait for the full ISR before calling a write delivered
    enable.idempotence    broker dedupes retries by (producer, partition, sequence)

And the client model bites everyone once: produce() only ENQUEUES into a local
buffer. poll() drives the network and fires delivery callbacks; flush() drains
the buffer before exit. Skip both and your script ends with messages that were
never sent — no error shown.

Each payload carries a global `seq` so the rebalance experiment can prove
no-loss / count-duplicates from the consumer ledgers.

Usage:
    python src/produce_orders.py --count 1000            # bounded, then histogram
    python src/produce_orders.py --count 1000 --keyless  # key=None comparison
    python src/produce_orders.py --rate 200              # continuous (Ctrl-C stops)
    python src/produce_orders.py --rate 50 --message-timeout 10000  # broker-kill demo
"""

import argparse
import json
import random
import time

from confluent_kafka import Producer

from config import BOOTSTRAP, TOPIC_ORDERS, producer_summary_path

STATUSES = ["created", "paid", "shipped", "delivered"]

# 50 customers with a realistic skew: a handful of whales place most orders.
CUSTOMERS = list(range(50))
WEIGHTS = [1 / (i + 1) for i in CUSTOMERS]          # zipf-ish


# Stamped into every payload: the topic accumulates messages across runs, and
# seq restarts at 0 each run — without a run id, ledger analysis would count
# a previous run's seq=42 as a "duplicate" of this run's seq=42.
RUN_ID = int(time.time())


def make_order(seq: int) -> dict:
    return {
        "run": RUN_ID,
        "seq": seq,
        "order_id": seq,
        "customer_id": random.choices(CUSTOMERS, WEIGHTS)[0],
        "amount": round(10 + random.random() * 140, 2),
        "status": random.choice(STATUSES),
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def histogram(counts: dict[int, int]) -> str:
    total = sum(counts.values()) or 1
    lines = []
    for p in sorted(counts):
        n = counts[p]
        bar = "#" * max(1, round(n / total * 60))
        lines.append(f"P{p}  {bar}  {n}")
    return "\n".join(lines)


def run(count: int | None, rate: float, keyless: bool,
        message_timeout_ms: int | None = None) -> None:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                     # the producer's side of the triplet
        "enable.idempotence": True,        # default in 2.x — explicit on purpose
    }
    if message_timeout_ms:
        # Default delivery.timeout.ms is 300_000 (5 min): when writes can't meet
        # acks=all (broker-kill demo), the idempotent producer retries SILENTLY
        # for 5 minutes and the callback never fires. A short timeout makes it
        # give up fast and surface _MSG_TIMED_OUT to on_delivery — the visible
        # failure the broker-kill demo wants on screen.
        conf["message.timeout.ms"] = message_timeout_ms
    producer = Producer(conf)

    per_partition: dict[int, int] = {}
    errors = 0

    def on_delivery(err, msg):
        nonlocal errors
        if err:
            errors += 1
            print(f"FAILED: {err}")
        else:
            per_partition[msg.partition()] = per_partition.get(msg.partition(), 0) + 1

    mode = "key=None" if keyless else "key=customer_id"
    print(f"producing to '{TOPIC_ORDERS}' ({mode})"
          + (f", {count} records" if count else f", {rate}/s until Ctrl-C"))

    seq = 0
    try:
        while count is None or seq < count:
            order = make_order(seq)
            producer.produce(
                topic=TOPIC_ORDERS,
                key=None if keyless else str(order["customer_id"]).encode(),
                value=json.dumps(order).encode(),
                callback=on_delivery,
            )
            producer.poll(0)               # pump callbacks — NOT optional
            seq += 1
            if rate > 0:
                time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("\nstopping (flushing the buffer)...")

    producer.flush(30)                     # drain before exit — also not optional
    print(f"\ndelivered {sum(per_partition.values()):,} records, {errors} errors")
    print(histogram(per_partition))

    producer_summary_path().write_text(json.dumps({
        "run": RUN_ID, "produced": seq, "delivered": sum(per_partition.values()),
        "errors": errors, "per_partition": per_partition, "keyless": keyless,
    }))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Produce keyed order events")
    p.add_argument("--count", type=int, default=None, help="produce N then exit")
    p.add_argument("--rate", type=float, default=100.0,
                   help="msgs/sec pacing; 0 = as fast as possible")
    p.add_argument("--keyless", action="store_true", help="key=None (sticky partitioner)")
    p.add_argument("--message-timeout", type=int, default=None, metavar="MS",
                   help="producer message.timeout.ms; a low value (e.g. 10000) makes "
                        "the broker-kill demo surface _MSG_TIMED_OUT instead of "
                        "retrying silently for 5 minutes")
    args = p.parse_args()
    if args.count is None and args.rate <= 0:
        p.error("--rate must be > 0 for continuous mode")
    run(args.count, args.rate, args.keyless, args.message_timeout)
