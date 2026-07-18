"""Create the Kafka topics used by the Lesson 9 benchmark.

Usage:
    uv run python src/setup_topics.py
"""

from confluent_kafka.admin import AdminClient, NewTopic
from config import BOOTSTRAP, FLINK_RESULTS_TOPIC, ORDERS_TOPIC, SPARK_RESULTS_TOPIC


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = [
        NewTopic(ORDERS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(SPARK_RESULTS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(FLINK_RESULTS_TOPIC, num_partitions=1, replication_factor=1),
    ]
    admin.create_topics(topics)
    print(f"created topics: {ORDERS_TOPIC}, {SPARK_RESULTS_TOPIC}, {FLINK_RESULTS_TOPIC}")


if __name__ == "__main__":
    main()
