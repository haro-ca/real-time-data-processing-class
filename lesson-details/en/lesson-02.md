# Lesson 2, What happens when you distribute OLTP?

The question is: what do you pay to push past that ceiling?

## Hour 1, Theory: the fundamental costs of distribution

Don't start with CockroachDB or any specific system. Start with *why* distribution is hard, from first principles. Students who skip this end up thinking distributed databases are "just Postgres but more nodes" and they'll get destroyed later in the course when things fail in non-obvious ways.

### Module A, The network changes everything

Open with a latency comparison that should shock them into paying attention:

| Operation | Latency |
|---|---|
| Local memory access | ~100ns |
| SSD read | ~100us |
| Same-datacenter network round-trip | ~500us |
| Cross-region network | ~50-150ms |

A distributed transaction that requires coordination between nodes pays the network cost on the critical path. In Lesson 1, the bottleneck was often WAL fsync at ~10-50us per commit on NVMe. Now you're adding 500us minimum per coordination round. That's a 10-50x penalty just from physics, before any protocol overhead.

This is the framing for the entire lesson: **every mechanism you study today exists to either reduce the number of coordination rounds or tolerate the latency they introduce.**

### Module B, CAP theorem, done properly

Most students have seen the Venn diagram. That version is borderline useless. Teach it as Brewer actually intended and as Gilbert/Lynch formalized it:

CAP says during a network partition (P), you must choose between consistency (C) and availability (A). It says nothing about normal operation, during normal operation you can have both. The real insight is that partitions *will* happen (cables get cut, switches fail, GC pauses look like partitions to other nodes), so the question isn't "pick two", it's **"when a partition occurs, does the system refuse to serve requests (CP) or serve potentially stale data (AP)?"**

- **CockroachDB and YugabyteDB are CP systems**, during a partition, the minority side stops accepting writes.
- **Cassandra in its default config is AP**, it'll accept writes on both sides and reconcile later (last-write-wins or custom resolution).

This distinction matters enormously for OLTP, if you're tracking bank balances, AP with last-write-wins can literally lose money.

Also mention Kleppmann's critique: CAP is about a very specific kind of consistency (linearizability) and a very specific kind of availability (every non-failing node responds). Real systems operate in the space between these extremes. This nuance is what separates a senior engineer from someone who memorized a blog post.

### Module C, Consensus: how do nodes agree on anything?

The core problem: if you have 3 nodes and a client sends a write to node 1, how do nodes 2 and 3 learn about it and agree on the order of operations? This is consensus.

Teach Raft because it was designed to be understandable (that's literally in the Ongaro/Ousterhout paper title). Cover the key mechanics:

- **Leader election**, term numbers, vote requests, split-brain prevention
- **Log replication**, leader appends to its log, sends AppendEntries RPCs, waits for majority acknowledgment before committing
- **Safety**, the election restriction that ensures a new leader has all committed entries

**The critical performance insight:** every committed write requires a majority of nodes to acknowledge, that's at least one network round-trip to the followers. With 3 nodes you need 2 acknowledgments; with 5 nodes you still need only 3. Adding nodes improves fault tolerance (can lose more nodes) but doesn't increase the quorum size linearly, so latency doesn't scale as badly as students expect.

Don't just lecture this. Walk through a concrete scenario:

1. 3 nodes, leader gets a write, sends to followers, one follower is slow. Does the write commit? *(Yes, majority is 2, the leader plus one fast follower.)*
2. Now the leader dies. What happens? *(The follower with the complete log wins the election. The slow follower catches up.)*
3. Now the old leader comes back with an uncommitted entry the new leader doesn't have. What happens? *(It gets overwritten, the old leader truncates its log to match the new leader.)*

### Module D, Distributed transactions: 2PC and its discontents

Single-key writes are handled by Raft within a single Raft group. But what about a transaction that touches data on different nodes? Example: transfer $100 from account A (node 1) to account B (node 2). Both must succeed or neither.

**Two-Phase Commit:** coordinator sends `PREPARE` to all participants, each participant writes a prepare record to its WAL and replies `YES`/`NO`, coordinator collects votes, if all `YES`, sends `COMMIT`; if any `NO`, sends `ABORT`. Each participant commits or aborts locally.

**The problem:** if the coordinator dies after sending `PREPARE` but before sending `COMMIT`, all participants are stuck. They've promised to commit but don't know the decision. They hold locks and wait. This is the **blocking problem of 2PC**, and it's been known since the 1980s. It's real, in production, coordinator failure during the commit window causes exactly this kind of lock-up.

CockroachDB solves this with **parallel commits** (a technique where the coordinator writes the transaction status to a distributed record in parallel with the final batch of writes, so participants can discover the outcome independently). YugabyteDB uses a similar approach. The key insight: modern distributed SQL databases haven't eliminated 2PC, they've made the failure window vanishingly small and recoverable.

### Module E, Clocks and ordering

In a single Postgres, transaction ordering is trivial, there's one incrementing XID counter. In a distributed system, who's to say transaction 1000 on node A happened before transaction 1001 on node B? Physical clocks drift. NTP can be off by milliseconds.

Cover three approaches briefly:

- **Lamport clocks**, logical, no relation to wall time. Sufficient for causal ordering.
- **Hybrid logical clocks (HLC)**, combines physical time with logical counters. What CockroachDB uses.
- **TrueTime**, Google Spanner's hardware-based approach using GPS and atomic clocks to bound clock uncertainty. CockroachDB doesn't have this luxury, so it uses a "clock skew tolerance" parameter and waits out the uncertainty window.

The practical consequence: CockroachDB has a `--max-offset` flag (default 500ms). If node clocks drift beyond that, the node self-terminates to prevent consistency violations. Students will see this if they mess with container clocks in the practical.

---

## Hour 2, Practical: deploy, benchmark, compare

### Setup (15 min)

Provide a Docker Compose file with a 3-node CockroachDB cluster. Students shouldn't spend time debugging cluster setup, that's ops, not the learning objective. The Compose file should expose the SQL port and the admin UI (port 8080, which gives them a built-in dashboard for ranges, latency, and QPS).

Use the same `orders` table schema from Lesson 1. This is deliberate, same schema, same workload, different architecture. The comparison must be apples-to-apples.

### Phase 1, Baseline single-region benchmark (20 min)

Students port their `asyncpg` load generator from Lesson 1 to use CockroachDB's Postgres-compatible wire protocol. (CockroachDB speaks the Postgres wire protocol, so `asyncpg` works with minor adjustments, connection string changes, and a few Postgres-specific features won't work.)

Run the same INSERT workload at the same concurrency levels (10, 50, 200, 500). Record TPS and latency percentiles. **Results will be worse than single-node Postgres**, probably 2-5x lower TPS and significantly higher p99 latency. Students who expected "more nodes = more performance" get their first surprise.

Why is it slower? Each INSERT goes through Raft consensus, the leaseholder (like a Raft leader for that key range) must replicate to at least one other node before acknowledging. That's one extra network hop on the critical path. Have them verify this in the CockroachDB admin UI, look at the Raft latency metrics.

### Phase 2, Understand the range architecture (20 min)

This is where CockroachDB's design becomes tangible. Data is split into **ranges** (default 512MB). Each range is a Raft group replicated across 3 nodes. As students INSERT more data, new ranges are created and the system rebalances them across nodes.

Tasks:

1. Insert 10M rows (batched, to be reasonable on time).
2. Inspect the admin UI to see how many ranges the `orders` table has been split into and how they're distributed across nodes.
3. Query `crdb_internal.ranges` to see the range boundaries.

**Key question:** if one node holds the leaseholder for most ranges, what happens to your throughput? *(It bottlenecks, all writes funnel through that node's Raft leadership.)*

Then demonstrate what CockroachDB does about this: lease rebalancing. Show `SHOW RANGES FROM TABLE orders` and observe lease distribution. If it's uneven, wait and watch it rebalance, or discuss rebalancing policies.

### Phase 3, Kill a node (20 min)

This is the exercise students remember.

Start the INSERT workload at steady-state concurrency (~50 coroutines). While it's running, `docker stop` one of the three nodes. Students must observe and record:

- What happens to TPS?
- Does it drop to zero? For how long?
- What do the error logs in the client say?

**Expected behavior:** brief latency spike (seconds), some connection errors if the client was connected to the killed node, but the cluster continues serving reads and writes because 2 of 3 nodes are still up (majority quorum). TPS recovers, possibly at slightly lower throughput because the surviving nodes are handling more work.

Then bring the node back. Watch it catch up via Raft log replay. Observe `crdb_internal.gossip_liveness`, the node goes from decommissioning/dead back to live. **Key teaching point:** the system automatically healed. No manual intervention. This is what you're buying with the latency penalty from Phase 1.

### Phase 4, Kill two nodes (15 min)

Now `docker stop` two of three nodes. The cluster should become **unavailable for writes**, it's lost quorum. The client gets errors. This is the CP guarantee in action: the system refuses to serve potentially inconsistent data. It chose consistency over availability.

Ask: what would an AP system (Cassandra) do here? *(Accept the writes on the surviving node, deal with conflicts later.)* Which is correct for an OLTP banking workload? *(CP, almost certainly, you can't have two nodes independently accepting transfers on the same account.)*

---

## Hour 3, Advanced experiments and the honest tradeoff analysis

### Experiment A, Distributed transactions across ranges (25 min)

Design a workload that forces distributed transactions: a money transfer between two accounts that are on different ranges (different nodes). The simplest way:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

Where `id = 1` and `id = 2` are on different ranges (students verify this using `SHOW RANGES`). Benchmark this vs. single-range transactions (both accounts on the same range). The distributed transaction will be measurably slower, this is the 2PC overhead from Module D, made visible.

Then push it harder: run 50 concurrent transfer pairs. Some of them will contend on the same accounts. Observe **transaction retries**, CockroachDB uses serializable isolation by default and will force retries on conflict. Students must implement retry logic in their python code (CockroachDB's docs have the retry wrapper pattern, but they should understand *why* it's needed: serializable isolation + optimistic concurrency control = aborted transactions that must be retried at the application layer).

### Experiment B, Latency injection to simulate multi-region (20 min)

Use `tc` (traffic control) in the Docker containers to add 50ms latency between nodes, simulating cross-region deployment. Re-run the benchmark. TPS should crater. This makes the theoretical latency discussion from Module A viscerally real.

Ask: if you deployed this across US-East, US-West, and EU-West, every write commit would pay ~100-150ms of cross-region latency. At that point, what's your maximum theoretical TPS per Raft group?

Roughly `1000 / (round_trip_ms * coordination_rounds)`, maybe 3-7 commits per second per Raft group for cross-region. You can get more total throughput by having many ranges, but any single hot key is capped.

This is where students learn that **distributed databases don't magically solve the performance problem, they solve the availability problem at a significant latency cost.**

### Experiment C, Clock skew (15 min)

Manually skew the system clock on one Docker container using `faketime` or `date -s`. If you push it beyond CockroachDB's `--max-offset` (default 500ms), the node should self-terminate or refuse to join the cluster.

This drives home Module E, clocks aren't just theoretical, they're an operational concern. In production, NTP misconfiguration has taken down CockroachDB clusters.

### Final 20 minutes, Synthesis discussion

Put up a comparison table on the board, students fill it in from their own data:

| Dimension | Single Postgres (Lesson 1) | 3-node CockroachDB (Lesson 2) |
|---|---|---|
| TPS at 50 connections | | |
| p99 latency at 50 connections | | |
| Behavior when 1 node fails | | |
| Behavior when 2 nodes fail | | |
| Distributed transaction overhead | | |
| Operational complexity | | |

Then pose the Lesson 2 question: **when should you reach for a distributed OLTP database instead of scaling a single Postgres vertically?**

The answer isn't "when you need more throughput", a single Postgres on good hardware often beats a 3-node CockroachDB cluster on raw TPS. The answer is:

- When you need **fault tolerance across failure domains** and are willing to pay the latency premium
- When your **data volume exceeds what a single node can store**
- When you need **multi-region writes**

If you just need more TPS, look at read replicas, connection pooling, or application-level sharding first.

This is a crucial takeaway. Too many engineers jump to distributed systems when they haven't exhausted single-node optimization. Lesson 1 showed them the ceiling; Lesson 2 shows them the cost of the distributed alternative. That tension drives the rest of the course.

---

## Take-home deliverable

A comparative analysis report: single-node Postgres (Lesson 1) vs. 3-node CockroachDB (Lesson 2). Must include:

- **Latency histograms** (not averages, CDFs or at minimum p50/p95/p99)
- **TPS curves** across concurrency levels
- **Failure behavior documentation** (what they observed killing nodes)
- A **one-page architectural recommendation** answering: *"For an OLTP workload doing 20k TPS with 99.99% availability requirements, which would you choose and why?"*

The recommendation must reference their own benchmark data, not generic blog post wisdom.
