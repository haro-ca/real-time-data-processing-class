"""Produce a steady stream of transactions into the `transactions` topic.
Run this in one terminal while the Spark job runs in another."""

import argparse
import json
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, DATA_DIR, PRODUCED_FILE, TRANSACTIONS_TOPIC, banner

CURRENCIES = ["USD", "EUR", "GBP"]
DEFAULT_CUSTOMERS = 1000


def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TRANSACTIONS_TOPIC in admin.list_topics(timeout=10).topics:
        return
    fs = admin.create_topics([NewTopic(TRANSACTIONS_TOPIC, num_partitions=4,
                                       replication_factor=1)])
    for _, f in fs.items():
        try:
            f.result(timeout=30)
            print(f"created topic '{TRANSACTIONS_TOPIC}'")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def write_produced(total: int, customer_count: int, tps: int) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    PRODUCED_FILE.write_text(json.dumps({
        "total": total,
        "customer_count": customer_count,
        "tps": tps,
        "stopped_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2))


def run(customer_count: int, tps: int, duration: int | None) -> None:
    ensure_topic()
    banner("seed_transactions",
           f"producing {tps} transactions/sec to '{TRANSACTIONS_TOPIC}'",
           f"duration: {'unlimited' if duration is None else f'{duration}s'}" ,
           "customer_id is keyed by customer so the stream-static join is deterministic")

    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})
    interval = 1.0 / tps
    produced = 0
    start = time.time()
    stop_requested = False

    def on_stop(signum, frame):
        nonlocal stop_requested
        stop_requested = True
        print("\n  signal received, finishing current batch...")

    signal.signal(signal.SIGINT, on_stop)
    signal.signal(signal.SIGTERM, on_stop)

    print("producing transactions (keyed by customer_id) ...")
    while not stop_requested:
        if duration is not None and time.time() - start >= duration:
            break
        txn = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"cust-{random.randint(0, customer_count - 1):06d}",
            "amount": round(random.uniform(1.0, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
            "transaction_time": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(TRANSACTIONS_TOPIC,
                         key=txn["customer_id"].encode("utf-8"),
                         value=json.dumps(txn).encode("utf-8"))
        producer.poll(0)
        produced += 1
        print(f"tx {produced:>6} {txn['transaction_id']} {txn['customer_id']} {txn['amount']:>9.2f} {txn['currency']}")
        time.sleep(interval)

        if produced % 100 == 0:
            write_produced(produced, customer_count, tps)

    producer.flush(30)
    write_produced(produced, customer_count, tps)
    print(f"produced {produced:,} transactions")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate transaction stream")
    p.add_argument("--customer-count", type=int, default=DEFAULT_CUSTOMERS)
    p.add_argument("--tps", type=int, default=50)
    p.add_argument("--duration", type=int, default=None,
                   help="seconds to run; default is unlimited")
    args = p.parse_args()
    run(args.customer_count, args.tps, args.duration)
