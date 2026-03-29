# Lesson 1, How much can a single OLTP node handle?

## Hour 1, Theory: What's actually happening inside Postgres when you INSERT a row

The goal of this hour isn't "learn Postgres." It's to build a mental model precise enough that students can predict which resource will saturate first under a given workload. That's the bar.

Start bottom-up, not top-down. Don't open with architecture diagrams, open with a single `INSERT INTO orders (...)` and trace what physically happens.

### The write path, step by step

A client connects → Postgres forks a backend process (this is expensive, one OS process per connection, not a thread). The `INSERT` is parsed, planned (trivial for a simple `INSERT`, but still goes through the planner), and executed. Execution means: allocate space in a heap page (8KB pages, tuple gets a CTID like `(42, 3)` meaning page 42, slot 3). The tuple header carries `xmin` (the transaction ID that created it), this is MVCC, the foundation of concurrency. Before the heap write is considered durable, the WAL entry is written to the WAL buffer and then flushed to disk (`fsync`). This is the durability guarantee. Only after the WAL flush does the transaction commit return to the client. The actual heap page modification happens in `shared_buffers` (RAM) and gets flushed to disk later by the background writer or checkpointer.

This sequence gives you five distinct bottleneck points to teach, and each one becomes a module:

### Module A, Connection overhead

One process per connection. Each backend process costs ~5–10MB of RSS. At 500 connections you're burning gigabytes just on process overhead, plus context switching kills you. This is why `pgbouncer` exists, and why connection pooling isn't optional at scale.

Show `pg_stat_activity`, students should see actual backend processes.

**Key insight to drive home:** the maximum useful concurrency is roughly `(CPU cores * 2) + effective_spindle_count`. Beyond that, you're just adding contention.

### Module B, WAL and the fsync bottleneck

Every commit needs a WAL flush. On a single spinning disk, that's ~100–200 fsyncs/second. On NVMe, maybe 100k+ IOPS. This is often the first bottleneck students will hit.

Explain `synchronous_commit` settings, turning it off gives you huge TPS gains but you can lose the last few milliseconds of committed transactions on crash. This is a real production tradeoff, not academic.

Also cover `wal_level` (`minimal`, `replica`, `logical`), logical is more expensive because it writes more detail, and it's what CDC in Lesson 5 will need.

### Module C, Shared buffers, the OS page cache, and the double-buffering problem

Postgres manages its own buffer pool (`shared_buffers`) but also relies on the OS page cache. If `shared_buffers` is too large, you're caching the same pages twice. The rule of thumb (25% of RAM) exists because of this.

Use `pg_buffercache` extension to actually see what's in the buffer pool. For write-heavy workloads, the checkpointer becomes critical, it writes dirty pages from `shared_buffers` to disk. If checkpoint I/O spikes, your write latency spikes.

### Module D, MVCC and vacuum as a concurrency cost

Every `UPDATE` in Postgres doesn't modify the existing tuple, it creates a new tuple and marks the old one dead (`xmax`). This means UPDATE-heavy workloads create dead tuples that accumulate. Table bloat grows. Sequential scans get slower because they're scanning dead rows.

Vacuum reclaims this space, but it has a cost, it does I/O, takes locks (lightweight, but still), and competes with your workload. Autovacuum's default settings are conservative. An aggressive write workload can outpace vacuum, leading to bloat spiral.

Students need to see this happen, not just hear about it, that's what the practical exercise is for.

### Module E, Lock contention

Row-level locks, table-level locks, advisory locks. For OLTP, the typical killer is row-level contention, many transactions trying to `UPDATE` the same hot row (think: inventory counter, account balance). Postgres uses a wait queue. Beyond a certain concurrency, you spend more time waiting for locks than doing useful work.

Show `pg_stat_activity` with `wait_event_type = 'Lock'`.

### The key question

After these five modules, pose the key question: **for a simple INSERT-only workload with no contention, which of these five saturates first?**

(Answer: almost always WAL fsync on slow storage, or CPU on fast storage with complex queries. Students should not know this yet, they'll discover it in the practical.)

---

## Hour 2, Practical: build a load generator, find the bottleneck

Here's why they build their own and don't use `pgbench`: pgbench teaches you nothing about instrumentation. The point isn't to get a TPS number, it's to *explain* the TPS number.

### Setup (15 min)

Postgres in Docker with resource limits (`--cpus=2 --memory=4g`). This is important, constraining resources makes bottlenecks appear faster and more clearly. Students should have a simple schema ready:

```sql
CREATE TABLE orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Phase 1, Naive load generator (20 min)

Students write a synchronous python script using `psycopg3` that inserts rows in a loop. Measure TPS. It'll be terrible, maybe 500–2000 TPS depending on hardware.

Why? One connection, synchronous commits, no pipelining. Each `INSERT` round-trips to the server and waits for WAL flush.

### Phase 2, Async with connection pooling (20 min)

Rewrite using `asyncpg` with N concurrent coroutines. Start with N=10, then 50, then 200, then 500. Students must record TPS and p50/p95/p99 latency at each concurrency level.

They'll see TPS climb, plateau, and then degrade as concurrency increases past the sweet spot. That degradation is the lesson, **more connections ≠ more throughput**.

### Phase 3, Instrument everything (20 min)

While the load generator runs:

**On the Postgres side**, students query:

- `pg_stat_statements`, total calls, `mean_exec_time`, `shared_blks_hit` vs `shared_blks_read`
- `pg_stat_bgwriter`, `buffers_checkpoint`, `buffers_clean`, `buffers_backend` (if `buffers_backend` is high, the backends are doing their own writes because the background writer can't keep up)
- `pg_stat_user_tables` for the orders table, `n_tup_ins`, `n_dead_tup`, `last_autovacuum`

**On the OS side:**

- `iostat -x 1` for disk utilization and await
- `vmstat 1` for context switches and CPU
- `htop` for per-process CPU

**On the python side:**

- `py-spy record` to generate a flame graph. Is the bottleneck in `asyncpg`? In the event loop? In network round-trips?

### Phase 4, Identify and articulate (15 min)

Students must write a one-paragraph hypothesis: *"The bottleneck is X because Y."*

For example: "The bottleneck is WAL fsync, `iostat` shows the disk at 98% utilization with high await, and increasing concurrency beyond 50 doesn't increase TPS because all backends are waiting for the same disk to flush."

---

## Hour 3, Push it further, then break it

This hour separates the students who followed a tutorial from the ones who understand the system.

### Experiment A, Batching

Instead of one `INSERT` per round-trip, use `executemany` or `COPY`. Measure the TPS difference. It should be dramatic (5–10x).

Why? Fewer round-trips, fewer WAL flushes (one per batch commit instead of one per row). Students should connect this back to Module B.

### Experiment B, Synchronous commit off

Set `synchronous_commit = off` at the session level. Re-run the load generator. TPS should jump significantly.

Then ask: **what did you just give up?**

Answer: you can lose up to `wal_writer_delay` worth of committed transactions on crash, typically 200ms. Make them calculate: at 50k TPS, that's 10k transactions you've "committed" to the client that might vanish. Is that acceptable? For what use cases?

### Experiment C, The hot row problem

Change the workload: instead of `INSERT`s, do `UPDATE orders SET amount = amount + 1 WHERE id = 1`, everyone hitting the same row.

Watch TPS collapse even at modest concurrency. Show `pg_stat_activity`, backends piling up in lock waits. This is contention, and no amount of hardware fixes it. The only solutions are architectural (optimistic locking, application-level sharding of the hot row, queuing the updates).

### Experiment D, Bloat

Run a heavy `UPDATE` workload for a few minutes, then check `pg_stat_user_tables.n_dead_tup`. Disable autovacuum (`ALTER TABLE orders SET (autovacuum_enabled = false)`), keep updating, and watch table size grow.

Then run a sequential scan and time it, it'll be slower than it should be because Postgres is scanning dead tuples. Re-enable vacuum, run `VACUUM VERBOSE orders`, and see the reclaimed space.

### Final 15 minutes, Class discussion

Given everything you've observed, what's the maximum TPS you could extract from a single Postgres node with sensible hardware (NVMe, 16 cores, 64GB RAM)? Not a guess, an estimate backed by the bottleneck analysis.

Reasonable answer for simple inserts: 100k–300k TPS with batching and fast storage. For complex transactional workloads with contention: 10k–50k TPS. The point is that the answer depends entirely on the workload shape.

---

## Take-home deliverable

A written report (not code, a report) with:

- Their bottleneck analysis per phase
- The flame graphs and `pg_stat_*` output as evidence
- Their final TPS estimate with reasoning
- One paragraph on what would happen if they needed 10x their observed maximum
