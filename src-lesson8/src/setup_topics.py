"""Create the Kafka topics for Lesson 8: a regular `transactions` topic and a
compacted `customers` topic. By default it resets the topics (delete+create) so
repeated runs are idempotent."""

import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, CUSTOMERS_TOPIC, TRANSACTIONS_TOPIC


def topic_exists(admin, name: str) -> bool:
    return name in admin.list_topics(timeout=10).topics


def wait_for_deletion(admin, name: str, timeout: int = 30) -> bool:
    for _ in range(timeout):
        if name not in admin.list_topics(timeout=10).topics:
            return True
        time.sleep(1)
    return False


def create_topics(reset: bool) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    for topic in (TRANSACTIONS_TOPIC, CUSTOMERS_TOPIC):
        if topic_exists(admin, topic):
            if reset:
                print(f"  deleting existing topic '{topic}'")
                fs = admin.delete_topics([topic], operation_timeout=30)
                for _, f in fs.items():
                    try:
                        f.result(timeout=30)
                    except Exception as e:
                        if "UNKNOWN_TOPIC_OR_PART" not in str(e):
                            raise
                if not wait_for_deletion(admin, topic):
                    raise RuntimeError(f"topic '{topic}' did not delete in time")
            else:
                print(f"  topic '{topic}' already exists; skipping")
                continue

        if topic == CUSTOMERS_TOPIC:
            new_topic = NewTopic(
                topic,
                num_partitions=4,
                replication_factor=1,
                config={
                    "cleanup.policy": "compact",
                    "min.cleanable.dirty.ratio": "0.01",
                    "segment.ms": "100",
                },
            )
            label = "compacted (table)"
        else:
            new_topic = NewTopic(topic, num_partitions=4, replication_factor=1)
            label = "regular (stream)"

        fs = admin.create_topics([new_topic])
        for _, f in fs.items():
            try:
                f.result(timeout=30)
                print(f"  created '{topic}' ({label})")
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    print(f"  '{topic}' already exists")
                else:
                    raise


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Create Lesson 8 Kafka topics")
    p.add_argument("--no-reset", action="store_true", help="do not delete existing topics")
    args = p.parse_args()

    print("Setting up Kafka topics for Lesson 8...")
    create_topics(reset=not args.no_reset)
    print("Done.")
