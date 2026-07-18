"""Create the Kafka topics used by the Lesson 9 benchmark.

Kafka retains topic data across benchmark runs even after `rm -rf ckpt`, so
re-running the benchmark without resetting topics silently mixes results from
old runs into the new analysis. Use --reset before a fresh measurement.

Usage:
    uv run python src/setup_topics.py
    uv run python src/setup_topics.py --reset   # delete + recreate first
"""

import argparse
import sys
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from config import BOOTSTRAP, FLINK_RESULTS_TOPIC, ORDERS_TOPIC, SPARK_RESULTS_TOPIC

TOPICS = [ORDERS_TOPIC, SPARK_RESULTS_TOPIC, FLINK_RESULTS_TOPIC]


def reset_topics(admin: AdminClient) -> None:
    futures = admin.delete_topics(TOPICS, operation_timeout=30)
    for name, future in futures.items():
        try:
            future.result()
            print(f"  deleted: {name}")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"  did not exist: {name}")
            else:
                print(f"  FAILED to delete {name}: {e}", file=sys.stderr)
    # Deletion is asynchronous on the broker; give it a moment before recreating.
    time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description="create (and optionally reset) L9 Kafka topics")
    parser.add_argument("--reset", action="store_true", help="delete existing topics before creating them")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    if args.reset:
        reset_topics(admin)

    topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in TOPICS]
    futures = admin.create_topics(topics)

    failed = False
    for name, future in futures.items():
        try:
            future.result()
            print(f"  created: {name}")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"  already exists: {name}")
            else:
                print(f"  FAILED: {name}: {e}", file=sys.stderr)
                failed = True

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
