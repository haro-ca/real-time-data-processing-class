# Lesson 12, Capstone: break everything, fix everything

This is the final exam, but it doesn't look like one. Students get the instructor's reference pipeline from Lesson 11, the full stack running end-to-end: Postgres (OLTP) -> Debezium (CDC) -> Kafka -> Spark Structured Streaming -> ClickHouse (OLAP) -> API layer. Everything works. Then you break it, six different ways, and they have to find and fix each failure under time pressure.

The pedagogical goal is diagnostic reasoning. Building a pipeline teaches you how things work. Debugging a broken pipeline teaches you how things fail, and the failure modes are what you'll actually deal with in production. Every scenario in this lesson is something that has taken down real systems at real companies. No contrived puzzles.

## Hour 1, Theory: why things break and how to think about it (45 min)

Keep this tight. Students are itching to get to the CTF. The theory exists to give them a mental framework for systematic diagnosis, not to delay the fun.

### Module A, Failure taxonomy for streaming systems

Present the taxonomy as layers, because that's how you diagnose, top-down from symptoms to root cause, or bottom-up from infrastructure to application:

**Layer 1, Infrastructure failures:**
- Disk full (writes fail silently or loudly depending on the system)
- Network partitions (nodes can't reach each other, split-brain scenarios)
- Clock skew (ordering violations, certificate expiry, timeout miscalculations)
- OOM kills (the kernel kills your process, no graceful shutdown)

**Layer 2, Data platform failures:**
- Kafka broker going offline (under-replicated partitions, leader elections)
- Consumer group rebalancing storms (consumers join/leave repeatedly, no progress)
- Replication slot bloat in Postgres (WAL accumulates because the CDC consumer fell behind)
- ClickHouse merge storms (too many parts from high-frequency inserts)

**Layer 3, Application/data failures:**
- Poison pill messages (one malformed event crashes the processor on every restart)
- Schema incompatibility (producer changes field types, consumer silently drops or misinterprets data)
- Partition hotspots (one Kafka partition gets all the traffic, one consumer does all the work)
- Backpressure collapse (upstream produces faster than downstream can consume, buffers fill, everything stalls)

The key insight: **production failures almost never announce themselves clearly.** A disk full on the Kafka broker manifests as increased producer latency, then as consumer lag, then as stale dashboard data. The person looking at the dashboard doesn't know the disk is full, they see "the numbers stopped updating." Diagnosis means working backwards from symptoms through layers.

### Module B, Chaos engineering principles

Chaos engineering is not "randomly break stuff and see what happens." It's a disciplined practice with a specific methodology:

1. **Define steady state**, what does "working" look like in measurable terms? For this pipeline: end-to-end latency < 10s, consumer lag < 1000 events, ClickHouse query latency < 500ms, zero error rate in the API.
2. **Hypothesize**, "if we kill one Kafka broker, the system will recover within 30 seconds because topic replication factor is 3."
3. **Inject the failure**, actually kill the broker.
4. **Observe**, did reality match the hypothesis? If not, you found a weakness.
5. **Fix and harden**, improve the system so the hypothesis holds next time.

Netflix's Chaos Monkey is the famous example, but the principle applies at every scale. The students' CTF exercise is essentially steps 3-5 without step 2, they don't know what's broken, so they have to diagnose before they can hypothesize.

### Module C, Graceful degradation patterns

When failure happens, you have three options (in order of preference):

1. **Automatic recovery**, the system heals itself. Kafka leader election, Spark checkpoint recovery, consumer group rebalancing. This is what good architecture buys you.
2. **Graceful degradation**, the system continues operating at reduced capacity or freshness. Serve stale data from cache, drop non-critical events, switch to a simpler processing path. The dashboard shows data from 5 minutes ago instead of 5 seconds ago, not ideal, but the business keeps running.
3. **Controlled failure**, the system stops cleanly rather than producing wrong results. Circuit breakers, dead-letter queues, explicit error responses. Better to show "data unavailable" than to show wrong numbers.

The worst outcome, worse than downtime, is **silently serving incorrect data.** A dashboard that shows stale numbers without indicating staleness is more dangerous than a dashboard that's down, because people make decisions based on wrong data without knowing it's wrong. This is why monitoring and alerting aren't optional, they're part of the system.

### Module D, Capacity planning for streaming systems (brief)

Quick framework for sizing streaming systems, because several of the CTF failures stem from capacity issues:

- **Kafka:** storage = `throughput_bytes_per_sec * retention_seconds * replication_factor`. At 10MB/s with 7 days retention and RF=3, that's ~18TB. If you provisioned 5TB, you'll learn about it on day 3 at 2am.
- **Stream processor:** memory = `state_size + buffer_size + overhead`. Stateful joins hold data in memory/RocksDB. If your join window is 1 hour and throughput is 10k events/sec at 1KB each, that's ~36GB of state. Size accordingly or get OOM-killed.
- **ClickHouse:** ingestion throughput depends on batch size and merge capacity. Inserting one row at a time triggers a merge storm. Inserting 10k-row batches every second is fine. The difference isn't obvious until the system falls over.

These are napkin-math numbers, but napkin math prevents the most common capacity failures. Most teams that get paged at 2am never did the napkin math.

---

## Hour 2 + Hour 3, CTF: six failures, six post-mortems (2 hours 15 min)

### Format

Students work in pairs. Each pair gets the same Docker Compose environment: the instructor's reference pipeline from Lesson 11, running and processing events. The pipeline has been pre-loaded with baseline data and is in steady state, events flowing from Postgres through CDC, through Kafka, through Spark, into ClickHouse, queryable via the API.

Six failure scenarios have been injected into the environment. Each failure is independent, they affect different components and don't interact. Students can tackle them in any order. Each failure is designed to be diagnosable and fixable in 20-30 minutes by someone who has completed the course.

Students receive:

- A monitoring dashboard (Grafana) showing the usual metrics: consumer lag, processing throughput, ClickHouse query latency, API error rate, end-to-end latency
- A brief description of the **symptom** for each failure (what the user/operator sees), not the cause
- Access to all logs, metrics, configs, and code in the pipeline
- AI assistance is explicitly encouraged, Claude, Copilot, whatever. In production you'd use every tool available; same here

For each failure, students must produce:

1. **Root cause**, what's actually broken and why
2. **Detection method**, how they found it (which logs, metrics, queries, commands)
3. **Fix**, the actual code/config change that resolves the issue
4. **Prevention strategy**, what monitoring, validation, or architectural change would prevent this in the future

Time budget: 20-25 minutes per failure, 15 minutes for wrap-up discussion. In practice, some failures will take 10 minutes and some will take 30. That's realistic, in production, some incidents are obvious once you look at the right metric, and some require deep investigation.

### Failure 1, The poison pill (Spark layer)

**Symptom the student sees:** Spark Structured Streaming job is in a crash loop. It starts, processes a few micro-batches, then throws an exception and restarts. Consumer lag on the Kafka topic is growing steadily. The Grafana dashboard shows processing throughput dropped to zero and lag is climbing.

**What's actually broken:** A single event in the Kafka topic has a malformed JSON payload, a string value in a field that the Spark schema expects as an integer (`"amount": "not_a_number"`). The Spark job deserializes the event, attempts to cast the field, and throws a `NumberFormatException`. Because Spark's default behavior on a corrupt record is to fail the task, and the task retries hit the same record, the job enters an infinite crash loop. Critically, the offset never advances past this event because the micro-batch fails before committing.

**Why this is realistic:** Poison pills are one of the most common streaming failures. Any pipeline that trusts upstream data to always match the expected schema will eventually hit this. It's especially insidious because restarting the job doesn't help, the bad event is still there, and the consumer resumes from the last committed offset, which is right before the poison pill.

**The fix:** There are two parts. The immediate fix: configure Spark's `columnNameOfCorruptRecord` option (or use `PERMISSIVE` mode in `from_json`) to route malformed records to a dead-letter column instead of crashing. Quarantine the bad event, advance past it. The structural fix: add schema validation at the deserialization boundary with explicit error handling, malformed events go to a dead-letter topic, valid events proceed. Never let a single bad event block the entire pipeline.

**Detection path the student should follow:** Check Spark driver logs -> see the `NumberFormatException` with the specific field and value -> inspect the Kafka topic at the failing offset using `kafka-console-consumer --offset <offset> --partition <partition>` -> see the malformed payload -> understand why the offset can't advance.

**Prevention strategy:** Schema validation at ingestion (schema registry enforcement on the producer side), dead-letter queue pattern on the consumer side, alerting on consumer group lag rate of change (not just absolute lag, a lag that's *growing* is worse than a stable lag of 10k).

---

### Failure 2, Partition hotspot causing consumer lag spiral (Kafka layer)

**Symptom the student sees:** One of the Kafka consumer partitions has lag growing much faster than the others. Overall throughput looks okay in aggregate, but the Grafana dashboard shows end-to-end latency for some events is minutes while others are seconds. The API returns stale data for a subset of entities.

**What's actually broken:** The Kafka producer is using a custom partitioning key, `customer_id`. The data generator has been modified so that 80% of events come from a single customer ID (simulating a large enterprise client or a bot). Because Kafka partitioning is hash-based on the key, all these events land on the same partition. That partition's consumer is overwhelmed while the other consumers are idle. The consumer can't keep up, lag grows, and the problem compounds because Spark's micro-batch processes from all partitions but the skewed partition dominates each batch's processing time.

**Why this is realistic:** Partition skew is endemic in production Kafka deployments. Any workload partitioned by a business key (customer ID, merchant ID, device ID) is vulnerable. A single large customer, a load test pointed at one account, or a bot generating events under one ID can create this pattern. It's worse than it sounds because the skew often appears gradually, you ship the partition key logic on Monday, the large customer signs up on Wednesday, and by Friday night you're getting paged.

**The fix:** Immediate: rebalance the load by changing the partition key to something with more uniform distribution. Options include compound keys (`customer_id + event_id` or `customer_id + timestamp_suffix`), explicit partition assignment that spreads hot keys across partitions, or simply using a random partition key if ordering per-customer isn't required. If per-customer ordering is required, the fix is to sub-partition: use `customer_id` for coarse routing but allow multiple partitions per customer via a secondary hash. Alternatively, increase the partition count and scale out consumers, but this is a band-aid if the skew is extreme (one key will still land on one partition).

**Detection path:** Check per-partition consumer lag in Grafana (or via `kafka-consumer-groups --describe`) -> see one partition with 10x the lag of others -> inspect partition message counts -> see the skew -> check the producer's partitioning logic -> identify the hot key.

**Prevention strategy:** Monitor per-partition lag (not just aggregate lag). Alert on skew ratio (max partition lag / median partition lag > threshold). Profile partition key cardinality and distribution before going to production. Consider including partition-key distribution metrics in the producer's health checks.

---

### Failure 3, Replication slot bloat eating Postgres alive (CDC/Postgres layer)

**Symptom the student sees:** The Postgres OLTP database is running out of disk space. Write queries are getting slower. The monitoring dashboard shows Postgres disk usage climbing steadily. Eventually, if left unchecked, Postgres will refuse writes entirely.

**What's actually broken:** The Debezium CDC connector was paused (simulating a deployment gone wrong, a connector crash that wasn't restarted, or a network issue between Debezium and Kafka). While the connector is paused, the Postgres logical replication slot remains active, Postgres must retain all WAL segments since the slot's last confirmed LSN. WAL files accumulate. They can't be recycled because the replication slot tells Postgres "I still need those." The WAL directory grows without bound.

This is one of the most dangerous CDC failure modes because the feedback loop is catastrophic: the CDC consumer is down, so WAL accumulates, which fills the disk, which causes Postgres (the source OLTP system) to stop accepting writes. Your real-time analytics pipeline just took down your production database.

**Why this is realistic:** This happens constantly. A Debezium connector fails, the ops team doesn't notice (or notices but deprioritizes it because "it's just analytics"), and hours or days later Postgres runs out of disk. Every team that runs CDC from Postgres learns this lesson, usually the hard way.

**The fix:** Immediate: restart the Debezium connector so it starts consuming WAL and the replication slot LSN advances. Postgres will then recycle the old WAL segments. If the connector can't be restarted quickly (e.g., Kafka is also down), the emergency option is to drop the replication slot (`SELECT pg_drop_replication_slot('debezium');`), this lets Postgres recycle WAL immediately, but you'll need to re-snapshot the database when CDC is re-established. That's painful but better than a OLTP outage.

Structural fix: set `max_slot_wal_keep_size` in Postgres (available since PG13) to cap how much WAL a slot can retain. When the limit is hit, the slot is invalidated rather than filling the disk. You lose CDC continuity, but you protect the primary. Also configure disk space alerts with enough lead time to act.

**Detection path:** Check Postgres disk usage (`df -h` on the data directory, or `pg_database_size`) -> see the WAL directory (`pg_wal/`) is enormous -> check replication slots (`SELECT * FROM pg_replication_slots;`) -> see the slot's `confirmed_flush_lsn` is far behind `pg_current_wal_lsn()` -> check Debezium connector status -> find it's not running.

**Prevention strategy:** Monitor replication slot lag (the gap between `confirmed_flush_lsn` and current WAL position). Alert when it exceeds a threshold (e.g., 1GB of WAL retained). Set `max_slot_wal_keep_size`. Monitor Debezium connector health as a first-class production metric, if the connector is down, it's not just an analytics problem, it's a database risk.

---

### Failure 4, Schema evolution that silently drops fields (data quality/schema layer)

**Symptom the student sees:** The ClickHouse dashboard shows that a key metric, let's say `total_revenue`, has dropped to approximately zero over the last 30 minutes. The pipeline appears healthy: no errors in logs, consumer lag is normal, events are flowing, Spark is processing, ClickHouse is ingesting. Everything looks green. But the numbers are wrong.

**What's actually broken:** The Postgres source schema was altered, the `amount` column was renamed to `total_amount` (simulating a migration that a developer made to the OLTP schema without coordinating with the data team). Debezium picked up the schema change and started producing events with `total_amount` instead of `amount`. The Spark job, however, still references the field `amount` in its transformations. Because the Spark job uses a permissive schema (or because the JSON deserialization produces a null for the missing `amount` field rather than failing), the pipeline doesn't crash. It just reads `null` for every `amount` value, which the aggregation treats as zero.

This is the most dangerous failure in the entire CTF because there are no errors anywhere. Logs are clean. Metrics are green. The pipeline is happily processing garbage. The only signal is that a business metric changed in a way that doesn't match reality.

**Why this is realistic:** Silent schema incompatibility is a leading cause of data quality incidents. It's especially common in organizations where the OLTP team and the data team don't share a schema contract. A column rename, a type change from integer to string, a new enum value, any of these can propagate through CDC and either crash the pipeline (the good outcome) or silently corrupt downstream data (the bad outcome). The silent case is far worse because the blast radius grows until someone notices.

**The fix:** Immediate: update the Spark job to reference `total_amount` instead of `amount` (or add a mapping that handles both field names for backward compatibility). Backfill the ClickHouse data for the affected time window by replaying from Kafka (the events with `total_amount` are correct, they just weren't being read properly).

Structural fix: implement a schema registry (Confluent Schema Registry or equivalent) with compatibility checks. The Kafka topic should enforce a schema contract, if the producer tries to publish events with a different field name, compatibility checking rejects the schema change (or at least flags it as a breaking change). The Spark job should validate incoming schema against its expected schema at startup and fail loudly if there's a mismatch. Add data quality assertions downstream: if `sum(amount)` drops by more than 50% compared to the previous hour, fire an alert.

**Detection path:** This one is hard, and that's the point. The student has to notice that the business metric is wrong, not that the pipeline is broken. Look at ClickHouse query results -> see revenue dropped -> check if event volume changed (it didn't, same number of events) -> inspect a sample event in Kafka -> see the field is now `total_amount` -> check the Spark transformation code -> find it references `amount` -> understand the silent null behavior.

**Prevention strategy:** Schema registry with compatibility enforcement. Data quality monitoring (anomaly detection on key business metrics). Column-level lineage tracking. Mandatory schema review process for OLTP migrations that affect CDC topics. A simple `NOT NULL` assertion in the Spark job would have turned this into a loud failure instead of a silent one.

---

### Failure 5, ClickHouse merge storm from micro-inserts (OLAP layer)

**Symptom the student sees:** ClickHouse queries that normally return in 200ms are now taking 10-30 seconds. The API layer is timing out. ClickHouse CPU usage is pegged at 100%. The `system.merges` table shows dozens of active merges. Looking at `system.parts`, the number of active parts for the target table is in the thousands.

**What's actually broken:** The Spark-to-ClickHouse sink was reconfigured (simulating a bad config change) to flush every single micro-batch as a separate insert, with each micro-batch containing only 10-50 rows. ClickHouse's MergeTree engine creates a new "part" (a directory of column files) for each insert. With hundreds of inserts per minute, each creating a tiny part, the background merge process can't keep up. The number of parts grows, and ClickHouse must check all active parts for every query (each part is scanned independently and results are merged). Query performance degrades proportionally to the number of parts. Eventually, ClickHouse may refuse inserts entirely with "Too many parts" error.

**Why this is realistic:** This is the single most common ClickHouse operational mistake. ClickHouse's documentation warns about it explicitly, but people still do it. It happens when developers treat ClickHouse like a row-oriented database and insert one row at a time, or when a streaming sink is configured with too-small batch sizes. The MergeTree engine is optimized for bulk inserts (thousands to millions of rows per insert), violating that assumption causes cascading degradation.

**The fix:** Immediate: stop the micro-inserts. Manually trigger a merge (`OPTIMIZE TABLE ... FINAL`), this is expensive but will consolidate parts. Be careful: `OPTIMIZE ... FINAL` on a large table can take a long time and consume significant I/O, so it might be better to optimize specific partitions.

Structural fix: configure the Spark sink to buffer and batch writes to ClickHouse, minimum 10k-100k rows per insert, or time-based batching (e.g., flush every 5-10 seconds, whichever accumulates more rows). Use ClickHouse's `Buffer` table engine as an intermediate layer that absorbs high-frequency small inserts and flushes to the MergeTree table in larger batches. Set `parts_to_throw_insert` to a reasonable limit so ClickHouse rejects inserts before the situation becomes unrecoverable rather than degrading silently.

**Detection path:** Check ClickHouse slow query log or `system.query_log` -> see query times spiking -> check `system.parts` for the target table (`SELECT count() FROM system.parts WHERE table = 'events' AND active`) -> see thousands of active parts -> check `system.merges` -> see the merge process is overwhelmed -> check the ingestion pattern -> find the tiny inserts.

**Prevention strategy:** Monitor active part count per table. Alert when it exceeds a threshold (e.g., 300 parts, ClickHouse's default `parts_to_throw_insert` is 300, so alert well before that). Enforce minimum batch sizes in the sink configuration. Document the "no micro-inserts" rule in the pipeline's CLAUDE.md so AI assistants and future developers don't accidentally regress it.

---

### Failure 6, Kafka disk full with no retention enforcement (Kafka layer + infrastructure)

**Symptom the student sees:** Kafka producers are getting errors, `NotEnoughReplicasException` or `KAFKA_STORAGE_ERROR`. New events from CDC are being dropped. The pipeline from Kafka onward is still processing old events, but no new data is arriving. Consumer lag appears stable (because new events aren't being produced, the lag counter isn't increasing, this is a misleading signal).

**What's actually broken:** Kafka's `log.retention.bytes` was set to `-1` (unlimited) and `log.retention.hours` was set to `8760` (one year), simulating a configuration done by someone who wanted to "keep everything" without calculating the storage implications. The Kafka data directory has filled the disk. When Kafka can't write to disk, brokers start rejecting produce requests. Depending on `min.insync.replicas` settings and the broker that's full, this can take down the entire cluster for writes even if other brokers have space (because all replicas of a partition must be writable for the partition to accept writes if `acks=all`).

This failure pairs with Failure 3 (replication slot bloat) to illustrate a pattern: **unbounded retention anywhere in the pipeline is a ticking time bomb.** Postgres WAL, Kafka segments, ClickHouse parts, every component that retains data needs a retention policy with a storage cap.

**Why this is realistic:** Kafka disk exhaustion is a top-5 production incident across the industry. Teams set long retention because "we might need to replay," don't monitor disk usage, and then discover at 3am that the cluster is down. The misleading consumer lag signal makes it worse, the on-call engineer looks at lag, sees it's stable, and concludes the pipeline is fine. It's not fine. It's not producing.

**The fix:** Immediate: reduce retention to free disk space. Set `log.retention.hours` to a reasonable value (e.g., 72 hours) and `log.retention.bytes` to a per-partition cap. Wait for Kafka's log cleaner to delete old segments and free space. If the disk is 100% full and Kafka can't even start its cleanup, you may need to manually delete old log segments from the filesystem (risky, only delete segments older than what consumers need).

Structural fix: set `log.retention.bytes` per-topic based on the capacity planning math from Module D. Monitor Kafka broker disk usage. Alert at 70% disk utilization, page at 85%. Use tiered storage (Kafka's `remote.log.storage` in newer versions) to offload old segments to S3 if you genuinely need long retention. Configure `log.retention.check.interval.ms` to a frequent-enough value that cleanup doesn't lag behind production.

**Detection path:** Check Kafka producer logs -> see storage errors -> check Kafka broker logs -> see disk full messages -> `df -h` on the Kafka data directory -> confirm disk at 100% -> check `log.retention.bytes` and `log.retention.hours` in broker config -> find the unlimited retention policy -> calculate expected disk usage using Module D's formula -> confirm it exceeds available disk.

**Prevention strategy:** Capacity planning at deployment time (Module D). Disk usage monitoring with tiered alerting (warning at 70%, critical at 85%). Per-topic retention policies based on actual replay needs, not "keep everything." Automated alerting on producer error rates, if producers are failing, that's an immediate signal regardless of what consumer lag looks like.

---

### CTF logistics and timing

**0:00–0:10**, Orientation. Walk students through the running pipeline. Show them the Grafana dashboard in steady state. Explain the rules: six failures, work in pairs, any order, AI tools welcome. Each failure needs a post-mortem write-up plus the actual fix (code change or config change committed to their fork).

**0:10–2:00**, Active CTF. Students work through the failures. Instructor circulates, gives hints if a team is stuck for more than 10 minutes on a single failure (the goal is learning, not frustration), and ensures teams don't just fix the symptom without understanding the cause. If a team finishes all six early, have them review another team's post-mortems, the review itself is educational.

**2:00–2:15**, Wrap-up discussion. Go through each failure as a class. For each one, ask: "Who found this one hardest? What was the misleading signal that sent you down the wrong path?" The Failure 4 (silent schema drop) will reliably generate the best discussion because it's the one where every instinct says "the pipeline is fine" but the data is wrong.

Close with the meta-lesson: **the most dangerous failures are the ones that don't set off alarms.** Crashes are easy, the system tells you it's broken. Silent data corruption, gradual degradation, and misleading metrics are hard. The skills that matter most aren't "fix the crash", they're "notice that something is wrong when nothing is obviously wrong" and "design systems that fail loudly instead of silently."

---

## Take-home deliverable

A pull request to the student's fork of the course repository containing:

**1. Post-mortem documents**, one per failure (six total). Each post-mortem must include:

- **Title and severity** (P1-P4, with justification for the severity rating)
- **Timeline**, when the failure started, when the student detected it, how long diagnosis took
- **Root cause**, what broke and why, at a technical level specific enough that another engineer could reproduce it
- **Detection method**, the exact commands, queries, log lines, and metrics that led to diagnosis. Not "I checked the logs", which log, which line, what it said
- **Fix**, the specific code or configuration change, with a diff
- **Prevention strategy**, what monitoring, alerting, validation, or architectural change would either prevent the failure or detect it automatically within minutes

**2. Code fixes**, the actual changes to the pipeline code and configuration that resolve each failure, committed as separate, well-described commits. The code must be runnable, the instructor should be able to apply the fixes and verify the pipeline recovers.

**3. Repository structure**, the PR must include a `README.md` summarizing all six failures and a `CLAUDE.md` (or `AGENTS.md`) documenting the pipeline architecture, common failure modes, and debugging runbook so that an AI coding assistant working in the repository would have the context needed to diagnose similar issues in the future.

AI assistance is explicitly encouraged for all parts of the deliverable, diagnosis, code fixes, and post-mortem writing. The post-mortems must still demonstrate genuine understanding (you can't fake the detection timeline if you include actual timestamps and commands), but using AI to help structure the analysis, suggest prevention strategies, or write the prose is expected and mirrors real incident response workflows.
