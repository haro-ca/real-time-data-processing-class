# Lesson 6, Event streaming fundamentals (Kafka)

Lesson 5 gave students CDC, a stream of changes coming out of Postgres. But that stream has a problem: it's tightly coupled to the source database. One consumer falls behind, and the replication slot retains WAL segments until the source runs out of disk. Two consumers that want the same stream? Two replication slots, double the load on Postgres. You need a buffer, something that decouples producers from consumers, retains data durably, and lets multiple consumers read at their own pace without affecting each other or the source. That's Kafka.

But Kafka is not a message queue. This distinction matters and students will get it wrong if you don't establish it immediately. RabbitMQ delivers a message to a consumer and deletes it. Kafka writes a message to an append-only log and never deletes it (until retention expires). Consumers track their own position in the log. This is a fundamentally different data structure with fundamentally different properties, and the entire lesson is built on making that concrete.

## Hour 1, Theory: the commit log and everything that follows from it

### Module A, Log-structured storage: why an append-only log is the foundation

Open with the data structure, not the product. A log is an ordered, append-only sequence of records. Each record gets a monotonically increasing sequence number. You can only append to the end; you never modify existing records. That's it. This is the simplest useful distributed data structure, and Kafka's entire design follows from it.

Why append-only? Because appending to the end of a file is the cheapest possible I/O operation. No seeking, no random writes, no rewriting existing data. Sequential disk writes on modern hardware are absurdly fast, a single NVMe drive can sustain 2-3 GB/s sequential writes. Kafka exploits this: it writes incoming messages sequentially to segment files on disk, and reads are also sequential (consumers read from a position forward). The OS page cache does the rest, frequently accessed segments stay in RAM without Kafka managing any cache itself.

This is why Kafka can sustain millions of messages per second on modest hardware. It's not magic, it's the consequence of aligning the data structure with what disks are good at.

Compare this to a traditional message broker like RabbitMQ. RabbitMQ maintains per-message state (delivered? acknowledged? requeued?), uses indexes to track which messages are pending for which consumer, and deletes messages after acknowledgment. That's random I/O. At high throughput, the broker becomes the bottleneck. Kafka sidesteps this entirely: the broker doesn't track per-consumer state at all. It just appends to the log and serves reads from offsets.

**Key insight to plant here:** this append-only log is the same idea behind the PostgreSQL WAL from Lesson 1 and the CDC stream from Lesson 5. WAL is a commit log for a single database. Kafka is a commit log as a distributed service. Students should see the conceptual continuity, they've been working with logs all along.

### Module B, Topics, partitions, and offsets

A **topic** is a named log, `orders`, `user-events`, `cdc.public.orders`. But a single log on a single machine doesn't scale. So Kafka splits each topic into **partitions**, independent logs, each stored on a potentially different broker.

The key properties:

1. **Within a partition, ordering is guaranteed.** Record at offset 42 was written before record at offset 43. Always. This is the only ordering guarantee Kafka makes.
2. **Across partitions, there is no ordering guarantee.** A record in partition 0 at offset 100 has no defined temporal relationship to a record in partition 1 at offset 100.
3. **Each record in a partition gets a unique, monotonically increasing offset.** The offset is just an integer, it's the consumer's position in the log.

A **producer** decides which partition a record goes to. By default, if a record has a key, Kafka hashes the key and assigns it to `hash(key) % num_partitions`. Records with the same key always go to the same partition, this gives you per-key ordering. If there's no key, records are distributed round-robin (or sticky-batch in modern clients for better throughput).

This partitioning model is why Kafka scales horizontally: you increase throughput by adding partitions (and brokers to host them). But it comes at the cost of global ordering. Students who need total ordering across all events are limited to a single partition, which limits throughput to what one broker can handle. This is a fundamental tradeoff, not a configuration knob.

Draw this on the board (or have students draw it):

```
Topic: orders (3 partitions)

Partition 0:  [offset 0] [offset 1] [offset 2] [offset 3] ...
Partition 1:  [offset 0] [offset 1] [offset 2] ...
Partition 2:  [offset 0] [offset 1] [offset 2] [offset 3] [offset 4] ...

Producer with key="customer-42" → hash("customer-42") % 3 = 1 → always Partition 1
Producer with key="customer-77" → hash("customer-77") % 3 = 0 → always Partition 0
```

### Module C, Consumer groups and the consumption model

A **consumer group** is a set of consumers that cooperate to read a topic. Kafka assigns each partition to exactly one consumer in the group. If you have 3 partitions and 2 consumers in a group, one consumer gets 2 partitions and the other gets 1. If you have 3 partitions and 4 consumers, one consumer sits idle, you can never have more active consumers than partitions.

This is the scaling model for consumption: more partitions = more consumers = more throughput. But it also means consumer count is bounded by partition count, so you must choose partition count at topic creation with some foresight.

**Offset commits:** each consumer periodically tells Kafka "I've processed up to offset X in partition Y." This is called committing offsets. On restart, the consumer resumes from the last committed offset. Two critical behaviors:

- **Auto-commit** (`enable.auto.commit=true`, the default): the client automatically commits offsets every 5 seconds. This is convenient but dangerous, if the consumer crashes after auto-commit but before it actually processed those messages, those messages are lost (never reprocessed). If it crashes before auto-commit, it reprocesses messages (duplicates).
- **Manual commit**: the consumer explicitly calls `commit()` after processing. This gives you control but you have to get it right. Commit before processing = at-most-once. Commit after processing = at-least-once (you might reprocess on crash). Exactly-once requires more machinery (covered in Module D).

**Rebalancing:** when a consumer joins or leaves the group (or crashes, or a new partition is added), Kafka reassigns partitions across the remaining consumers. This is a **rebalance**. During a rebalance, no consumer in the group can fetch messages, it's a stop-the-world event. The rebalance protocol:

1. A consumer detects a change (heartbeat timeout, new member joins).
2. The group coordinator (a broker) revokes all partition assignments.
3. Consumers get an `on_partitions_revoked` callback, **this is where you must commit offsets for any in-flight work.**
4. The coordinator reassigns partitions using the configured strategy (range, round-robin, cooperative-sticky).
5. Consumers get an `on_partitions_assigned` callback, initialize state for the new partitions.

If a consumer doesn't commit offsets in `on_partitions_revoked`, another consumer will pick up that partition and reprocess from the last committed offset. This is the most common source of duplicates in Kafka consumers, and it's what the deliverable tests.

**Cooperative sticky rebalancing** (the modern default) is an improvement: instead of revoking all partitions, it only revokes the ones that need to move. Consumers keep processing their stable partitions during the rebalance. Students should use the `CooperativeStickyAssignor` and understand why it exists.

### Module D, Exactly-once semantics: the full picture

Exactly-once in Kafka operates at three levels, and conflating them is a common mistake:

**Level 1, Idempotent producers.** A producer sends a record to a broker. The broker writes it and sends an ACK, but the ACK is lost. The producer retries, sending the same record again. Without idempotency, you get a duplicate in the log. With `enable.idempotence=true`, the producer attaches a sequence number per partition. The broker deduplicates based on `(producer_id, partition, sequence)`. Duplicates from retries are silently dropped. This is on by default in modern Kafka clients and has negligible performance cost, there's no reason to turn it off.

**Level 2, Transactional producers.** What if a producer needs to write to multiple partitions atomically? Example: you read from partition A, process, and write results to partitions B and C. You want all three writes (the offset commit for A and the records to B and C) to be atomic, all succeed or all fail. Transactional producers use a `transactional.id`, call `begin_transaction()`, do their writes, and call `commit_transaction()`. Kafka's transaction coordinator uses a two-phase commit protocol internally.

**Level 3, Consumer read isolation.** Consumers set `isolation.level` to `read_committed` to only see records from committed transactions. Without this, consumers see uncommitted records that might later be aborted. This is the consumer-side complement to transactional producers.

**End-to-end exactly-once** (consume-process-produce) requires all three: idempotent producer, transactional producer wrapping the offset commit and output writes in a single transaction, and downstream consumers using `read_committed`. This is the Kafka Streams model. Outside of Kafka Streams, achieving true exactly-once typically means idempotent producers + at-least-once delivery + idempotent consumers (the consumer can handle seeing the same record twice without incorrect side effects). This is the pragmatic approach for most python consumers.

### Module E, Replication and the ISR mechanism

Each partition is replicated across multiple brokers (typically replication factor = 3). One replica is the **leader**, all reads and writes go through it. The other replicas are **followers** that pull data from the leader.

The **ISR (In-Sync Replicas)** set is the subset of replicas that are caught up with the leader (within `replica.lag.time.max.ms`, default 30 seconds). When a producer sends a record with `acks=all`, the leader waits for all ISR members to acknowledge before confirming the write. If a follower falls behind, it's removed from the ISR, and the leader no longer waits for it.

The durability guarantee: as long as at least one ISR member survives, no committed data is lost. The setting `min.insync.replicas` controls the minimum ISR size for a write to be accepted, with `min.insync.replicas=2` and `acks=all`, you need the leader plus at least one follower to be in sync, or the broker rejects the write.

This is analogous to Raft quorum from Lesson 2, but not identical. Raft requires a majority to commit. Kafka's ISR is a dynamic set, it can shrink to just the leader if all followers are slow, and writes still succeed (unless `min.insync.replicas` prevents it). This is a design choice favoring availability over strict quorum: a Raft group with 2 of 3 nodes down can't write; a Kafka partition with `min.insync.replicas=1` can write with just the leader. The tradeoff is that if the leader then dies, you lose committed data.

**The practical config for durability in production:**

```
replication.factor=3
min.insync.replicas=2
acks=all
```

This means: 3 copies of each partition, writes require the leader + at least 1 follower to be in sync, and the producer waits for all ISR members to acknowledge. You can lose one broker and keep serving reads and writes. You cannot lose two without data loss risk.

Connect this back to the CockroachDB 3-node cluster in Lesson 2, same idea (survive one failure), different mechanism (ISR vs. Raft quorum).

---

## Hour 2, Practical: deploy Kafka and write producers/consumers

### Setup (15 min)

Kafka in KRaft mode, no ZooKeeper. This is the modern deployment model (production-ready since Kafka 3.3, ZooKeeper officially deprecated). KRaft means Kafka uses its own Raft-based consensus for metadata management (controller election, topic configuration, partition assignments) instead of delegating to ZooKeeper. Fewer moving parts, simpler operations.

Provide a Docker Compose file with a 3-broker KRaft cluster:

```yaml
# docker-compose.yml
services:
  kafka-1:
    image: apache/kafka:3.7.0
    container_name: kafka-1
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    ports:
      - "19092:9092"

  kafka-2:
    image: apache/kafka:3.7.0
    container_name: kafka-2
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    ports:
      - "29092:9092"

  kafka-3:
    image: apache/kafka:3.7.0
    container_name: kafka-3
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    ports:
      - "39092:9092"
```

Students shouldn't spend time debugging cluster formation. Verify the cluster is healthy:

```bash
# Create a test topic
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --create --topic test \
    --partitions 6 --replication-factor 3

# Describe it, verify ISR shows all 3 brokers per partition
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --topic test
```

Install the python client: `pip install confluent-kafka`. Not `kafka-python`, it's unmaintained and lacks critical features. `confluent-kafka` is a wrapper around `librdkafka` (C library), which means it's fast and supports the full Kafka protocol.

### Phase 1, Producer basics and partition behavior (20 min)

Students create a topic `orders` with 6 partitions and write a producer that generates order events:

```python
from confluent_kafka import Producer
import json
import socket

conf = {
    "bootstrap.servers": "localhost:19092",
    "client.id": socket.gethostname(),
    "acks": "all",
    # Idempotence is on by default in confluent-kafka >= 2.x
    # but be explicit so students see it
    "enable.idempotence": True,
}

producer = Producer(conf)


def delivery_callback(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(
            f"OK: topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


for i in range(1000):
    order = {
        "order_id": i,
        "customer_id": i % 50,  # 50 distinct customers
        "amount": round(10.0 + (i % 100) * 1.5, 2),
        "ts": f"2024-01-15T10:00:{i % 60:02d}Z",
    }

    # Key determines partition, same customer always same partition
    producer.produce(
        topic="orders",
        key=str(order["customer_id"]).encode("utf-8"),
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_callback,
    )

    # Don't forget: produce() is async. poll() triggers callbacks.
    producer.poll(0)

# Flush remaining messages
producer.flush()
```

**What students must observe and record:**

1. The delivery callbacks show which partition each record landed in. Records with the same `customer_id` key always go to the same partition. Records with different keys are distributed across partitions.
2. Offsets within a partition are sequential, 0, 1, 2, 3, ...
3. The `poll(0)` call is essential. `confluent-kafka` uses an internal send buffer. `produce()` enqueues the message; `poll()` triggers the delivery callbacks and handles broker communication. If you never call `poll()`, your callbacks never fire and the internal queue can overflow.

**Exercise:** produce 1000 records with keys, then produce 1000 records with `key=None`. Compare the partition distribution. With keys, some partitions have more records than others (hash distribution isn't perfectly uniform with 50 keys across 6 partitions). Without keys, the distribution should be nearly uniform (sticky partitioner batches to one partition, then rotates).

### Phase 2, Consumer basics and the poll loop (20 min)

Now consume those events:

```python
from confluent_kafka import Consumer, KafkaError
import json

conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "order-processor-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # we'll commit manually
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, not an error, just no more messages
                print(
                    f"Reached end of {msg.topic()}[{msg.partition()}] "
                    f"at offset {msg.offset()}"
                )
            else:
                raise Exception(msg.error())
        else:
            order = json.loads(msg.value().decode("utf-8"))
            print(
                f"Received: partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key().decode('utf-8')} order_id={order['order_id']}"
            )
            # Commit after processing
            consumer.commit(asynchronous=False)
finally:
    consumer.close()
```

**Critical teaching points:**

1. `auto.offset.reset="earliest"` means: if this consumer group has no committed offsets, start from the beginning of the topic. The alternative is `"latest"` (start from new messages only). This only applies on the first run, after that, the consumer resumes from committed offsets.
2. `enable.auto.commit=False`, students must commit manually. This is the foundation for the deliverable.
3. `consumer.poll(timeout=1.0)`, the poll loop is the heartbeat. If `poll()` isn't called within `max.poll.interval.ms` (default 5 min), the broker assumes the consumer is dead and triggers a rebalance. Long-running processing between polls is a classic cause of unwanted rebalances.
4. Committing after every message (`consumer.commit()` inside the loop) is correct for learning but terrible for performance. In production, you batch commits, process N messages, then commit. Students will optimize this in Phase 3.

**Exercise:** run the consumer, observe it read all 1000 messages. Stop the consumer. Run it again. It should resume from where it left off (no re-reading) because offsets are committed. Then start a *second* consumer in the same group in a separate terminal. Watch the rebalance: partitions get redistributed, and now each consumer handles a subset of partitions.

### Phase 3, Rebalance callbacks: the deliverable foundation (20 min)

This is the most important phase. Students implement rebalance callbacks that correctly commit offsets when partitions are revoked:

```python
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import time

# Track per-partition state
partition_message_counts = {}


def on_revoke(consumer, partitions):
    """Called when partitions are about to be taken away."""
    print(f"REVOKING: {[f'{p.topic}[{p.partition}]' for p in partitions]}")
    # Commit offsets for partitions being revoked
    # This is critical, if you don't commit here, the new owner
    # will reprocess from the last committed offset
    offsets = consumer.position(partitions)
    if any(o.offset >= 0 for o in offsets):
        consumer.commit(offsets=offsets, asynchronous=False)
        print(f"  Committed offsets: {[(o.partition, o.offset) for o in offsets]}")
    # Clean up per-partition state
    for p in partitions:
        partition_message_counts.pop(p.partition, None)


def on_assign(consumer, partitions):
    """Called when new partitions are assigned."""
    print(f"ASSIGNED: {[f'{p.topic}[{p.partition}]' for p in partitions]}")
    for p in partitions:
        partition_message_counts[p.partition] = 0


conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "order-processor-v2",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "partition.assignment.strategy": "cooperative-sticky",
}

consumer = Consumer(conf)
consumer.subscribe(["orders"], on_assign=on_assign, on_revoke=on_revoke)

try:
    batch = []
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # No message, good time to commit any pending batch
            if batch:
                consumer.commit(asynchronous=False)
                batch = []
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise Exception(msg.error())

        order = json.loads(msg.value().decode("utf-8"))
        partition_message_counts[msg.partition()] = (
            partition_message_counts.get(msg.partition(), 0) + 1
        )

        batch.append(msg)
        # Commit every 100 messages
        if len(batch) >= 100:
            consumer.commit(asynchronous=False)
            batch = []

finally:
    consumer.close()
```

**The experiment that makes rebalancing real:**

1. Start the producer sending messages continuously (one per 10ms).
2. Start consumer A. It gets all 6 partitions.
3. Start consumer B (same group). Watch the rebalance, `on_revoke` fires on A, `on_assign` fires on both.
4. Kill consumer B (`Ctrl+C`). Watch the rebalance, A gets the partitions back.
5. Check the output: were any messages duplicated? Were any lost? If `on_revoke` committed correctly, there should be no duplicates (beyond the at-least-once inherent in the commit-after-process pattern).

Students must run this experiment and record what happens with and without the `on_revoke` commit. Without it, they'll see the new consumer reprocess messages that the old consumer already handled but didn't commit.

---

## Hour 3, Advanced experiments and the bridge to Spark

### Experiment A, Consumer lag: measuring how far behind you are (20 min)

Consumer lag is the most important operational metric for Kafka consumers. It's the difference between the latest offset in a partition (the log-end offset) and the consumer's committed offset. If lag is growing, your consumer can't keep up with the producer.

Students build lag reporting into their consumer:

```python
from confluent_kafka import Consumer, TopicPartition
import time


def report_lag(consumer, topic="orders"):
    """Query broker for lag on all assigned partitions."""
    assignment = consumer.assignment()
    if not assignment:
        return

    # Get the committed offsets
    committed = consumer.committed(assignment, timeout=5.0)

    # Get the high watermark (latest offset) for each partition
    lag_report = []
    for tp in committed:
        # lo and hi watermarks from the partition metadata
        lo, hi = consumer.get_watermark_offsets(tp, timeout=5.0)
        committed_offset = tp.offset if tp.offset >= 0 else 0
        lag = hi - committed_offset
        lag_report.append(
            {
                "partition": tp.partition,
                "committed_offset": committed_offset,
                "high_watermark": hi,
                "lag": lag,
            }
        )

    total_lag = sum(r["lag"] for r in lag_report)
    print(f"\n--- LAG REPORT (total: {total_lag}) ---")
    for r in lag_report:
        bar = "#" * min(r["lag"], 50)
        print(
            f"  P{r['partition']}: committed={r['committed_offset']} "
            f"hw={r['high_watermark']} lag={r['lag']} {bar}"
        )
    print()


# In the main poll loop, report lag every 5 seconds
last_lag_report = time.time()

# ... inside the while True loop:
if time.time() - last_lag_report > 5.0:
    report_lag(consumer)
    last_lag_report = time.time()
```

**The experiment:** start the producer at high throughput (no sleep between produces). Start a consumer that adds a `time.sleep(0.01)` per message to simulate slow processing. Watch lag grow. Then start a second consumer in the same group, lag should stabilize or decrease as throughput doubles. This makes the relationship between partition count, consumer count, and lag viscerally clear.

**Key question to pose:** if your consumer lag is growing by 1000 messages/second and you currently have 3 consumers for 6 partitions, what are your options? *(Add up to 3 more consumers. Beyond 6 consumers, you need more partitions, which requires topic reconfiguration and triggers rebalancing. Or make each consumer faster.)*

### Experiment B, Out-of-order events and why they happen (15 min)

Students will produce events that are logically ordered by timestamp but arrive at different partitions:

```python
import json
import time
import random
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:19092",
    "acks": "all",
    "enable.idempotence": True,
})

events = [
    {"order_id": 1, "event": "created", "ts": "2024-01-15T10:00:00Z", "customer_id": 42},
    {"order_id": 1, "event": "paid",    "ts": "2024-01-15T10:00:05Z", "customer_id": 42},
    {"order_id": 1, "event": "shipped",  "ts": "2024-01-15T10:00:30Z", "customer_id": 42},
    {"order_id": 2, "event": "created", "ts": "2024-01-15T10:00:02Z", "customer_id": 77},
    {"order_id": 2, "event": "paid",    "ts": "2024-01-15T10:00:10Z", "customer_id": 77},
    {"order_id": 2, "event": "cancelled","ts": "2024-01-15T10:00:15Z","customer_id": 77},
]

# Shuffle to simulate events arriving out of logical order
random.shuffle(events)

for e in events:
    # Key by order_id, events for the same order go to the same partition
    producer.produce(
        topic="order-events",
        key=str(e["order_id"]).encode("utf-8"),
        value=json.dumps(e).encode("utf-8"),
    )
producer.flush()
```

**Observations students must make:**

1. Events for the same `order_id` are in the same partition, so within an order, the offset ordering matches the produce ordering, but the produce ordering was shuffled, so it does not match the timestamp ordering. Order 1's events might arrive as `paid`, `created`, `shipped` if that's how they were produced after the shuffle.
2. Events for different `order_id`s are in different partitions. A consumer reading partition 0 might see order 2's `cancelled` event before order 1's `created` event, even though `created` has an earlier timestamp.

**Key insight:** Kafka preserves *produce order*, not *event-time order*. If your system needs to process events in timestamp order, Kafka alone won't do it, you need a stream processing layer with event-time semantics and watermarks. That's Lesson 7. Plant this seed now.

Then ask: what would happen if we keyed by `customer_id` instead of `order_id`? For customers with multiple orders, events from different orders could interleave within the same partition. Keying strategy is an architectural decision with downstream consequences, students should think about it before producing a single message.

### Experiment C, Broker failure and ISR dynamics (10 min)

While the producer is running continuously:

1. `docker stop kafka-3`. Observe: the producer continues without interruption (with `acks=all` and `min.insync.replicas=2`, losing one of three brokers is fine, the ISR shrinks to 2).
2. Run `kafka-topics.sh --describe` and show that the ISR for some partitions now shows only 2 brokers.
3. `docker stop kafka-2`. Now only 1 broker remains. The ISR has only the leader. With `min.insync.replicas=2`, writes should fail, the producer gets `NOT_ENOUGH_REPLICAS` errors. This is the durability guarantee refusing to accept data it can't replicate safely.
4. Bring both brokers back. Watch the ISR repopulate. Producer resumes.

This directly parallels the CockroachDB node-kill experiment from Lesson 2. Same lesson: fault tolerance requires redundancy, and there's a minimum number of nodes below which the system correctly refuses to operate.

### The bridge to Lesson 7: from poll loops to Spark Structured Streaming (15 min)

This is where Lesson 6 stops feeling like an isolated Kafka tutorial and connects to the rest of the course. Students have spent two hours writing manual poll loops, managing offsets, handling rebalances, and tracking lag. Now show them the abstraction layer that does all of this for them, and explain what it's doing under the hood.

Put this mapping on the board:

| What you did manually in Lesson 6 | What Spark Structured Streaming does in Lesson 7 |
|---|---|
| `consumer.poll()` loop | Spark's micro-batch scheduler calls `poll()` for you on each trigger interval |
| Manual offset commits in `on_revoke` | Spark commits offsets to its checkpoint directory after each micro-batch completes, you never call `commit()` |
| `on_assign` / `on_revoke` callbacks | Spark manages partition assignment internally; no rebalance callbacks to write |
| `consumer.get_watermark_offsets()` for lag | Spark exposes `StreamingQueryProgress` with `inputRowsPerSecond`, `processedRowsPerSecond`, and offset ranges |
| JSON deserialization in consumer code | `from_json()` with a schema definition, applied as a DataFrame transformation |
| Keying and partition assignment reasoning | `repartition()` or shuffle by key in Spark, same concept, different vocabulary |

Show the Spark equivalent of what they built. Don't run it (no Spark cluster yet), just show the code:

```python
# This is Lesson 7 code, shown here as a preview, not an exercise.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("orders").getOrCreate()

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("customer_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("ts", StringType())

orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-1:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

query = (
    orders.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/spark-checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)
```

Walk through it line by line and point back to their manual code:

- `readStream.format("kafka")`, this creates a Kafka consumer internally, with its own consumer group, its own poll loop, its own offset management. All the machinery from Phase 2 and Phase 3 is happening inside that one line.
- `.option("startingOffsets", "earliest")`, this is `auto.offset.reset=earliest`.
- `checkpointLocation`, Spark writes offsets and processing state here instead of committing to Kafka's `__consumer_offsets` topic. This is why Spark can provide exactly-once: the checkpoint, the state update, and the output write are atomic.
- `trigger(processingTime="10 seconds")`, every 10 seconds, Spark runs a micro-batch: poll new records since the last offsets, process them as a batch DataFrame, write outputs, commit new offsets to the checkpoint. The poll loop you wrote by hand is now a scheduled micro-batch.

**The punchline:** everything students did manually today, the poll loop, offset commits, rebalance callbacks, lag tracking, still happens in Spark. Spark just automates it. The reason they wrote it by hand first is so that when Spark misbehaves (and it will, checkpoint corruption, offset mismatches, rebalance storms), they can diagnose the problem at the Kafka level instead of staring at an opaque "streaming query failed" error.

Close with: "Next week, you'll build a stream processing pipeline on top of this Kafka cluster. You'll never write a poll loop again. But you'll be glad you understand what's inside the one Spark writes for you."

---

## Take-home deliverable

A GitHub repo submitted via PR containing:

**1. Code, a consumer that handles rebalancing correctly and reports its own lag metrics.** Specifically:

- A producer that continuously generates order events (keyed by `customer_id`) to a partitioned topic.
- A consumer using `confluent-kafka-python` with:
  - Manual offset commits (no auto-commit).
  - `on_revoke` callback that commits offsets for in-flight work before partitions are taken away.
  - `on_assign` callback that initializes per-partition state.
  - Cooperative sticky assignment.
  - A lag reporter that periodically prints per-partition and total lag.
- A test script or instructions that demonstrate: start one consumer, start a second consumer (same group), kill the second consumer, output should show rebalance events with correct offset commits, and no message loss.

**2. README.md**, not a generic Kafka explainer. A concise document covering:

- How to run the code (Docker Compose up, install deps, run producer, run consumer).
- What the rebalance callbacks do and why they matter (in the student's own words).
- Observed lag behavior when adding/removing consumers.
- What happens when a broker is killed (with the actual error output pasted in).

**3. AGENTS.md (or CLAUDE.md)**, instructions for an AI coding assistant to understand, run, and modify this codebase. This is a deliberate meta-skill: students should be able to articulate their system's architecture in a way that an LLM can act on. Include: project structure, how to start the Kafka cluster, what each script does, the key configuration decisions (why 6 partitions, why `acks=all`, why manual commits), and known edge cases.

AI assistance is encouraged for writing the deliverable. The evaluation is on correctness of the rebalance handling and lag reporting, not on whether the student typed every character.
