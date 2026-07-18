"""Create the Kafka topics used by the Lesson 9 benchmark.

Usage:
    uv run python src/setup_topics.py
"""

import sys

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from config import BOOTSTRAP, FLINK_RESULTS_TOPIC, ORDERS_TOPIC, SPARK_RESULTS_TOPIC


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = [
        NewTopic(ORDERS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(SPARK_RESULTS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(FLINK_RESULTS_TOPIC, num_partitions=1, replication_factor=1),
    ]
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
