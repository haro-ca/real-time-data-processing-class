"""Seed the compacted `customers` topic and then periodically update a few
random customers. Run this in one terminal and leave it running while the rest
of the lesson executes."""

import json
import random
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, CUSTOMERS_TOPIC, banner, lesson

TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]
DEFAULT_COUNT = 1000


def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if CUSTOMERS_TOPIC in admin.list_topics(timeout=10).topics:
        return
    fs = admin.create_topics([NewTopic(CUSTOMERS_TOPIC, num_partitions=4,
                                       replication_factor=1,
                                       config={"cleanup.policy": "compact"})])
    for _, f in fs.items():
        try:
            f.result(timeout=30)
            print(f"created compacted topic '{CUSTOMERS_TOPIC}'")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def make_customer(i: int) -> dict:
    return {
        "customer_id": f"cust-{i:06d}",
        "name": f"Customer {i}",
        "tier": random.choice(TIERS),
        "region": random.choice(REGIONS),
    }


def run(count: int, update_interval: float, updates_per_batch: int) -> None:
    ensure_topic()
    banner("seed_customers",
           f"seeding {count:,} customers into compacted topic '{CUSTOMERS_TOPIC}'",
           f"then updating {updates_per_batch} customers every {update_interval}s")

    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})
    customers = [make_customer(i) for i in range(count)]

    for i, c in enumerate(customers):
        producer.produce(CUSTOMERS_TOPIC,
                         key=c["customer_id"].encode("utf-8"),
                         value=json.dumps(c).encode("utf-8"))
        if i % 100 == 0:
            producer.poll(0)
    producer.flush()
    print(f"seeded {count:,} customers")

    lesson(
        "The customers topic is compacted: Kafka will keep only the latest",
        "  value for each customer_id, so it behaves like a mutable lookup table.",
        "The Spark job will load a snapshot of this topic at startup.",
    )

    print(f"updating {updates_per_batch} customers every {update_interval}s (Ctrl-C to stop)...")
    try:
        while True:
            batch = random.sample(customers, k=min(updates_per_batch, len(customers)))
            for c in batch:
                c["tier"] = random.choice(TIERS)
                c["region"] = random.choice(REGIONS)
                producer.produce(CUSTOMERS_TOPIC,
                                 key=c["customer_id"].encode("utf-8"),
                                 value=json.dumps(c).encode("utf-8"))
                print(f"update {c['customer_id']}: tier={c['tier']}, region={c['region']}")
            producer.flush()
            time.sleep(update_interval)
    except KeyboardInterrupt:
        producer.flush()
        print("\nstopped customer updates")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Seed compacted customers topic")
    p.add_argument("--count", type=int, default=DEFAULT_COUNT)
    p.add_argument("--update-interval", type=float, default=5.0)
    p.add_argument("--updates-per-batch", type=int, default=5)
    args = p.parse_args()
    run(args.count, args.update_interval, args.updates_per_batch)
