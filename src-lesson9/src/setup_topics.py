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


def reset_topics(admin: AdminClient, poll_timeout: float = 20.0, poll_interval: float = 0.5) -> None:
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

    # future.result() resolving does not mean the topic is actually gone from
    # broker metadata yet — a fixed sleep here was NOT reliably long enough
    # and caused create_topics() below to silently hit TOPIC_ALREADY_EXISTS,
    # leaving the OLD topic (with old data) in place across "reset" runs.
    # Poll until the topics are confirmed absent instead of guessing.
    deadline = time.time() + poll_timeout
    still_present = TOPICS
    while time.time() < deadline:
        metadata = admin.list_topics(timeout=5)
        still_present = [t for t in TOPICS if t in metadata.topics]
        if not still_present:
            return
        time.sleep(poll_interval)
    print(f"  WARNING: still present after {poll_timeout}s: {still_present}", file=sys.stderr)


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
