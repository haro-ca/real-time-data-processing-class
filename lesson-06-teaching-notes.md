# Lesson 6 — Event Streaming (Kafka): Teaching Notes

> Live-teaching companion for `slides/lesson-06.html` (38 slides) and `src-lesson6/`.
> Built *with* Devin while walking the deck piece by piece. Each movement has:
> **the one thing to land**, **predict-first beats**, **demo commands**, **likely questions**, **timing**, and **the transition line**.

---

## 0. The spine of the whole lesson (say this out loud, more than once)

This lesson has TWO threads that everything hangs on. If students leave with only these, you won.

1. **"This is your third log."** Kafka isn't new — it's the append-only log they've used since Lesson 1, promoted to a shared service.
   - L1: Postgres **WAL** → durability (replay the log, recover the DB)
   - L5: WAL + **replication slot** → a change stream (replay the log, mirror the DB)
   - L6: **Kafka** → the log as a shared service (anyone can replay anything)

2. **"Process, then commit."** The watermark rule in its third costume. Same rule, three lessons:
   - L4 batch: record "loaded" in the **same txn** as the load
   - L5 CDC: **apply**, then advance the slot
   - L6 Kafka: **process**, then commit the offset

Recurring callbacks to name explicitly when they appear:
| Concept today | Callback to a past lesson |
|---|---|
| Sequential I/O is fast | L1: COPY beat row-by-row INSERT 50× |
| Hot key → hot partition | L1 hot row, L3 shard key |
| ISR durability floor | L2 CockroachDB / Raft quorum (survive 1 failure) |
| Idempotent sink saves you | L4 + L5 idempotency |
| Out-of-order events | L7 next week (watermarks) — today's topic is its lab rat |
| Exactly-once checkpoint | L8 |

---

## 1. Pre-class setup checklist

Run these BEFORE class so the live demos are instant (cold Docker pull mid-lecture kills momentum):

```bash
cd src-lesson6
docker compose up -d                       # 3 brokers (kafka-1/2/3) + runner, KRaft
uv run python src/create_topics.py         # orders (6 part, RF=3) + order-events (3)
uv run python src/create_topics.py --describe   # sanity: ISR shows all 3 per partition
uv run python src/produce_orders.py --count 10   # warm-up: first call can be slow due to coordinator load
```

- Brokers are on **localhost:19092 / 29092 / 39092** from the laptop.
- Spoiler reveals on slides: press **`r`** (or click) to reveal a `.spoiler` code block. Predict FIRST, then reveal.
- Have **3-4 terminals** pre-opened and labeled: `producer`, `consumer A`, `consumer B`, `lag/admin`.
- If brokers refuse to form a cluster after editing compose: `docker compose down -v` (stale volume keeps old CLUSTER_ID).
- Optional (circle-closer, needs L5 up): the Debezium overlay in `src-lesson6/debezium/`.

### Demo cheat-sheet (the order you'll actually run things)
```bash
# Movement 4 — build by hand
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
uv run python src/produce_orders.py --count 1000               # keyed histogram
uv run python src/produce_orders.py --count 1000 --keyless     # even spread
uv run python src/consume_naive.py                              # Ctrl-C, rerun → resumes

# Movement 5 — break it (5 failure modes)
# 1. A member joins (rebalance): 3 terminals
#    T1: uv run python src/produce_orders.py --rate 100
#    T2: uv run python src/consume_rebalance.py --name A
#    T3: uv run python src/consume_rebalance.py --name B (then SIGKILL B)
#    Repeat T3 with --skip-revoke-commit to see broken version
# 2. A member dies (lag): 4 terminals
#    T1: uv run python src/produce_orders.py --rate 100
#    T2: uv run python src/consume_rebalance.py --name A --slow 10
#    T3: uv run python src/watch_lag.py --group order-processor
#    T4: uv run python src/consume_rebalance.py --name B --slow 10
# 3. Events arrive out of order: 1 terminal
#    uv run python src/produce_out_of_order.py
#    uv run python src/produce_out_of_order.py --readback
# 4. A broker disappears: 2 terminals
#    T1: uv run python src/produce_orders.py --rate 50 --message-timeout 10000
#    T2: docker stop kafka-3 (ok) → docker stop kafka-2 (fails) → docker start kafka-2 kafka-3
```

---

## 2. Timing budget (3 × ~50 min, matches the deck's three "Parts")

| Block | Slides | Minutes | Notes |
|---|---|---|---|
| Hour 1 — Theory | 1–13 | ~50 | the mental model + the model + the hard parts |
| Hour 2 — Build by hand | 14–20 | ~50 | producer + naive consumer, live |
| Hour 3 — Break it + bridge | 21–33 | ~50 | rebalance/lag/disorder/kill, then Spark preview |
| Annex (reference) | 34–38 | as needed | run-it-yourself, prompts, gotchas, scripts |

---

# MOVEMENT 1 — Why Kafka / the mental model (slides 1–6)

**The one thing to land:** *A durable, replicated, append-only log placed BETWEEN producers and consumers, where each consumer owns its own position and retention is by TIME, not by acknowledgment.* Everything else in the lesson is a corollary of that one sentence.

### Slide 1 — Cover: "Decouple the stream."
- Tagline: CDC gave a change stream **welded to Postgres** — one slot per reader, slowest reader holds the source hostage. Kafka makes *the log itself* the product.
- The animated rails (P0/P1/P2 → offsets) preview partitions + per-partition offsets. Point at it: "same key, same partition, same order; everything else runs in parallel."
- **Teaching move:** don't explain partitions yet. Just plant "this is a log with positions."

### Slide 2 — Recap L5: "Real-time. Correct. *Welded to the source.*"
- The win from L5 was real: sub-second, deletes included, replay-safe.
- The cost: **1 slot per consumer**, **∞ WAL retained by the slowest reader**, **0 consumers can share a slot.**
- Money line: **"The OLTP database cannot be your buffer."**
- **Callback:** "You watched a forgotten slot eat the disk last week." Five teams = five slots = five WAL decodes = five ways to sink production.
- *(Optional live)* `experiment_second_slot.py` makes it visceral — two teams, two slots; one keeps pace (`team_fraud`, holds flat ~**352 bytes**), one is "down" (`team_search`, climbs to **hundreds of MB** in ~12s). Needs L5 Postgres up (`cd ../src-lesson5 && docker compose up -d`); ~12–20s; it self-cleans the slots it makes.
- **Two facts to say while it runs:**
  - **One slot = one reader.** A slot is a single cursor with a single position; Postgres allows only one *active* connection per slot, and `get_changes` consumes destructively. To give two teams the full stream you need two slots — N readers = N slots = N independent WAL decodes on the source. (Kafka's answer: data stored once, each *consumer group* keeps its own offset → free fan-out.)
  - **Why the healthy slot stays flat (impl detail, if asked):** draining a slot advances `confirmed_flush_lsn`, but the **`restart_lsn` that actually pins WAL on disk only moves at a CHECKPOINT.** The demo forces a checkpoint each report so `team_fraud`'s retention collapses; `team_search` has unconsumed WAL so the checkpoint can't free it.

### Slide 3 — The idea: "A log in the *middle.*"
- Producers **append and walk away**. Each consumer keeps its **own** position, reads at its own pace. A slow reader inconveniences nobody.
- **The hinge of the lesson:** retention is by **time (or size), not by acknowledgment.** Data isn't deleted because someone read it — it expires.
- Say it explicitly: *"That one design choice is what makes replay, fan-out, and late consumers all free."*

### Slide 4 — "This is your *third* log." (the L1→L5→L6 table — invert/dark slide)
- This is the emotional center of the deck. Slow down here.
- The same data structure every time: **ordered, append-only, position-addressed.**
- "You haven't left the idea since Lesson 1 — it just keeps getting promoted."
- **Teaching move:** ask the room to fill in the L6 row before showing it.

### Slide 5 — "Kafka is *not* a message queue."
- The single most important distinction; students get it wrong if you don't nail it now.
- Queue (RabbitMQ) vs Log (Kafka), row by row:
  - On read: deliver-then-**delete** vs **nothing happens to the data**
  - Position tracked by: the **broker, per message** vs the **consumer, one integer**
  - Broker I/O: **random** (acks/requeues/indexes) vs **sequential** append + read
  - Second consumer: **competes** for messages vs reads the **same log, own pace**
  - Replay yesterday: **impossible** vs trivial (`seek` to an offset)
- **Teaching move:** "Queue = a to-do list you cross items off. Log = a journal you can re-read from any page."

### Slide 6 — "Append-only is the *physics* answer." (why it's fast)
- Writes: append to end of a segment file. No seeks, no rewrites. NVMe ~2–3 GB/s sequential.
- Reads: also sequential, forward from a position; OS **page cache** serves hot segments — Kafka keeps no cache of its own.
- **No per-consumer state on the broker.** The bookkeeping a queue does per message simply doesn't exist.
- **Callback (L1):** COPY's sequential batches beat row-by-row INSERT 50× on the same disk. "Kafka is that lesson built into an architecture: align the data structure with what disks are good at."

### Predict-first beats in this movement
- Slide 4: have them guess the L6 row.
- Slide 5: ask "what happens to a Kafka message when you read it?" (expected wrong answer: "it's removed").

### Likely student questions (with answers)
- *"So the log grows forever?"* No — retention by time/size; old segments are deleted on a clock, regardless of who read them. (Compaction is a separate mode; mention only if asked.)
- *"If nothing is deleted on read, how does a consumer not re-read everything?"* It commits its offset; on restart it resumes there. (Full treatment on slide 10.)
- *"Isn't keeping all data expensive?"* Sequential storage is cheap; you tune retention. The win (fan-out, replay) usually dwarfs the disk cost.
- *"Is Kafka a database?"* No — no random reads/queries by key; it's an ordered log you scan forward. (Great segue to L7 "compute on it".)

### Common misconceptions to preempt
- "Kafka delivers messages to consumers" → No, consumers **pull** and track their own position.
- "A second consumer steals messages from the first" → only within the **same group**; different groups each get the whole stream (covered slide 9).

### Timing: ~12–15 min for all six. Don't over-dwell on 1–2; spend the time on 4 and 5.

### Transition line → Movement 2
> "We've agreed it's a log in the middle. Now: one log on one machine doesn't scale. So we cut it into pieces — and the way we cut it is the most consequential decision you'll make."

---

# MOVEMENT 2 — The partition model (slides 7–9)

**The one thing to land:** *Partitioning is the one knob that buys you parallelism — and it costs you global order. The KEY you choose decides who stays ordered, how load spreads, and how many consumers you can ever run. It's an architecture decision, made before the first message, expensive to undo.*

### Slide 7 — "Topic, split into *partitions.*"
- Define the three nouns cleanly:
  - **Topic** = a named log (`orders`).
  - **Partition** = an independent log, on a (potentially) different broker. One topic = N partitions.
  - **Offset** = just an integer; your position in *one* partition.
- The two guarantees — say them as a matched pair:
  - **Inside a partition, order is guaranteed.** Offset 42 was written before 43. Always. *(This is the ONLY ordering guarantee Kafka makes.)*
  - **Across partitions, there is NO order.** None. *"That's the price of parallelism."*
- Routing: **same key → same partition → per-key order.** No key → sticky batches → near-uniform spread.
- **Board drawing** (have them draw it): topic = 3 stacked rails of `[0][1][2]…`, with `hash(key) % 3` arrows landing keys on rails.

### Slide 8 — "The key is an *architectural* decision."
- Walk the three choices as trade-offs, not facts:
  - key = `customer_id` → all of a customer's events ordered, but a **whale concentrates load on one partition**.
  - key = `order_id` → per-order lifecycle ordered; a customer's orders may **interleave** across partitions.
  - need **total** order across everything? → **one partition = one broker's throughput.** "That ceiling is a law, not a config knob."
- **Callback (the big one):** they've met this disease twice — the **hot row** that collapsed Postgres in L1, the **shard key** that decided everything in L3. *"A hot key makes a hot partition — same disease, new organ."*
- Line to deliver: **"Choose keys the way you chose shard keys: by how the load distributes, not by what's convenient."**

### Slide 9 — "Consumer groups divide the partitions."
- **Group** = a team reading one topic together. Kafka gives each partition to **exactly one** consumer in the group.
- Different **groups** are invisible to each other — **each group gets the whole stream.** (This is the fan-out that cost a slot-per-reader in L5; here it's free.)
- The math, shown live-style:
  - 2 consumers / 6 partitions → 3 each
  - 3 consumers → 2 each
  - **7 consumers → six work, one sits idle ← the hard ceiling**
- **Parallelism = partition count.** Chosen at topic creation; changing it later reshuffles keys + triggers rebalances. "This is the capacity-planning decision of Kafka — make it with headroom."

### Predict-first beats
- Slide 7: "If I produce three events for `customer-42`, can they arrive out of order?" (No — same key, same partition.) "What about `customer-42` vs `customer-77`?" (Yes — different partitions, no guarantee.)
- Slide 9: "I have 6 partitions and start an 8th… I mean 8 consumers. What do the 7th and 8th do?" (Sit idle.)

### Likely student questions (with answers)
- *"Why not just always use one partition so everything's ordered?"* Then your whole topic's throughput = one broker, one disk. Order is the thing you give up to scale; pick where you can afford to.
- *"Can I add partitions later?"* Yes, but it re-maps `hash(key) % N`, so existing keys move partitions → per-key order breaks at the seam, and it triggers a rebalance. Plan with headroom instead.
- *"If I add a partition, do old messages move?"* No — old data stays put; only the routing of *new* keys changes. That's exactly why per-key ordering breaks across the change.
- *"Two teams want the same data — do they fight over it?"* No: put them in **different groups**, each gets the full stream independently.
- *"How many partitions should I pick?"* Rule of thumb: target peak throughput ÷ per-consumer throughput, with headroom; more partitions = more parallelism but more overhead/rebalance cost. (Don't over-promise a magic number.)

### Common misconceptions to preempt
- "More consumers always = more speed." Only up to **partition count**; past that they idle.
- "Kafka load-balances keys evenly." **Hashing is not balancing** — 50 keys over 6 partitions lands lumpy (you'll *prove* this on slide 18).
- "Different consumers of a topic compete." Only **within a group**. Across groups, everyone gets everything.

### Timing: ~12 min. Slide 8 is the keeper — spend the most time there; it's where the take-home keying decisions come from.

### Transition line → Movement 3
> "So a group splits the partitions and each consumer reads its share. But how does a consumer remember *where it was* — and what happens at the exact moment partitions move from one consumer to another? That handoff is where the duplicates live, and it's what your take-home is graded on."

# MOVEMENT 3 — The hard parts (slides 10–13)

**The one thing to land:** *The offset commit IS the watermark you've built twice — "process, then commit." The danger isn't a crash, it's a REBALANCE: work doesn't die, it gets reassigned, and the handoff (`on_revoke`) is where duplicates are born. "Exactly-once" is three separate claims, and durability (ISR) is a dial you set, not a law.*

> This is the densest block and the source of the deliverable. Don't rush slides 10–11. Slides 12–13 can be lighter (concept + callback) unless the room is hungry.

### Slide 10 — Offsets: "The same rule. *Third time.*" (the spine pays off)
- A consumer commits "I've processed up to offset X" to the internal topic `__consumer_offsets`; on restart it resumes there. **No manual bookkeeping** — `__consumer_offsets` is doing the L4 `pipeline_metadata` job.
- The payoff table — say "third costume" out loud:
  | Lesson | Watermark | The rule |
  |---|---|---|
  | L4 batch | `pipeline_metadata` table | record "loaded" in the **same txn** as the load |
  | L5 CDC | `confirmed_flush_lsn` | **apply**, then advance the slot |
  | **L6 Kafka** | **committed offset** | **process**, then commit the offset |
- The two failure modes, stated as a fork:
  - commit **before** processing → crash **loses** messages = **at-most-once**
  - commit **after** processing → crash **replays** a few = **at-least-once** (harmless if the sink is idempotent — their L5 move)
- **The auto-commit trap:** default `enable.auto.commit=true` commits on a **5-second timer, uncorrelated with your processing** — so it can land on *either* side of the fork unpredictably. *"We turn it off all lesson."*

### Slide 11 — Rebalancing: "partitions *move.*" (THE deliverable slide)
- Trigger: a consumer joins, crashes, or just **polls too slowly** → the group coordinator redistributes partitions.
- Two callbacks bracket the move; **one carries all the risk:**
  - `on_revoke` — partitions about to be taken. **Your last chance to commit in-flight work.** Skip it → the next owner reprocesses everything since your last commit.
  - `on_assign` — new partitions arrive; initialize their state.
- **Cooperative-sticky** (modern default): moves only the partitions that *must* move; the rest keep flowing. The old **eager** protocol froze the whole group on every change.
- Say it plainly: **"A botched `on_revoke` is the single most common source of duplicates in real Kafka systems — and it's exactly what your take-home is graded on."** We trigger one live in Part two (slide 23).
- **Reframe the mental model:** crash recovery you already know (resume from commit). The *new* failure mode is that your work doesn't crash — **it gets reassigned to someone else mid-flight.** `on_revoke` is the handoff note you leave the inheritor.

### Slide 12 — "Exactly-once" is *three* claims (demolish the marketing word)
- People conflate these three; separate them explicitly:
  | Level | Mechanism | Protects |
  |---|---|---|
  | 1 · Idempotent producer | `enable.idempotence`: broker dedupes by (producer, partition, sequence) | retried **sends** don't duplicate in the log — on by default, free, never disable |
  | 2 · Transactional producer | `transactional.id` + begin/commit (2PC inside Kafka) | writes across several partitions (incl. the offset commit) land **atomically** |
  | 3 · Read isolation | `isolation.level=read_committed` | consumers never see records from **aborted** transactions |
- **End-to-end exactly-once needs all three at once** = the Kafka Streams / Spark checkpoint model = **Lesson 8's whole subject.**
- **The pragmatic Python answer (say it as the takeaway):** idempotent producer + at-least-once delivery + **idempotent sink** — the thing they already live by since L4/L5.
- **Callback (L2):** 2PC reappears. Coordination still costs; the difference is you only pay it **inside one system** now.

### Slide 13 — ISR: "consensus with the *dial exposed.*" (durability)
- Each partition has a **leader** + **followers**. The **ISR** (in-sync replicas) = the subset currently caught up. With `acks=all`, a write is confirmed once **every ISR member** has it. The ISR **shrinks** when a follower lags — and that's the contrast with Raft.
- Two-column contrast (callback to L2 CockroachDB):
  | Raft (CockroachDB, L2) | ISR (Kafka) |
  |---|---|
  | quorum = fixed majority, always | ISR is a **dynamic set**, can shrink to 1 |
  | 2 of 3 down → refuses writes | `min.insync.replicas` is the **floor you choose** |
  | consistency wins, no dial | floor=1 → available, but a dead leader loses data |
- **The production durability triplet** (point back at the compose file — it's preconfigured):
  ```
  replication.factor  = 3     # three copies
  min.insync.replicas = 2     # refuse writes below leader+1
  acks                = all   # producer waits for the ISR
  # lose 1 broker: fine · lose 2: writes refuse, on purpose
  ```
- This is the theory behind **Break 04** (slide 27) — you'll *watch* the ISR shrink and writes get refused.

### Predict-first beats
- Slide 10: "Auto-commit fires every 5s. You crash 2s after processing 400 messages but before the timer. What happens on restart?" (You reprocess those 400 — or, if it *had* fired, you'd have lost the ones processed after.) The point: **you don't control which** — that's why we go manual.
- Slide 13: "RF=3, min.insync=2. One broker dies — writes? (fine). A second dies — writes?" (refused, on purpose). Saves the reveal for slide 27.

### Likely student questions (with answers)
- *"Where are committed offsets stored?"* In Kafka itself — the internal compacted topic `__consumer_offsets`, keyed by (group, topic, partition). Not in your DB, not on the consumer.
- *"If I just make my sink idempotent, do I even need exactly-once?"* For most Python pipelines, no — idempotent producer + at-least-once + idempotent sink is the pragmatic 95% answer. True EOS is for stateful stream processing (Streams/Spark), i.e. L8.
- *"Does cooperative-sticky mean no duplicates?"* No — it reduces *disruption* (fewer partitions move), not the need to commit on revoke. You still own correctness.
- *"What makes a consumer 'poll too slowly'?"* Exceeding `max.poll.interval.ms` between `poll()` calls → the broker assumes you're dead and evicts you → rebalance storm. Do heavy work in batches, poll often. (Annex gotcha.)
- *"Can the ISR really shrink to just the leader?"* Yes — and then with `min.insync.replicas=1` the write still succeeds, but if that leader dies you lose it. That's the availability-vs-durability dial in one sentence.

### Common misconceptions to preempt
- "Committing an offset means the message is done/acked and removed." No — commit only records *your position*; the data stays in the log until retention.
- "Idempotent producer = exactly-once." No — that's only Level 1 (no duplicate *sends*); it says nothing about your processing replaying after a rebalance.
- "acks=all means all 3 replicas." No — all replicas **currently in the ISR**, which may be fewer than 3. `min.insync.replicas` is the floor that makes "all" meaningful.
- "Kafka is CP like Raft." It's a tunable dial: with the triplet it behaves CP-ish; with `min.insync.replicas=1` it leans AP and can lose data.

### Timing: ~15–18 min. Spend it on 10 + 11. 12 and 13 are "name it, callback, move on" unless asked.

### Transition line → Movement 4 (build it by hand)
> "That's the entire theory: a log, cut into partitions, read by groups that commit their position, with durability you dial in. Now close the slides — we're going to *build* every piece of that by hand, so that when Spark hides the poll loop next week, you know exactly what it's hiding."

# MOVEMENT 4 — Build it by hand (slides 14–20)

**The one thing to land:** *Every piece of theory is now a line of Python you can see — produce → partition → commit → resume. The two things that bite: a producer that "sends" nothing because you forgot `flush()`, and the realization that the committed offset (in `__consumer_offsets`) is your L4 watermark, handed to you for free.*

> Part one of the deck. Close the slides and live-code / live-run. Demos verified on this cluster — real outputs below.

### Slide 14 — Divider: "Write the loop by hand."
- The framing that motivates the whole hour: **"No Spark, no Connect, no framework — so that next week, when Spark hides the poll loop, you know exactly what it's hiding."**

### Slide 15 — Setup: "Three brokers. No ZooKeeper."
- **KRaft** = Kafka runs its *own* Raft for cluster metadata (the L2 algorithm, now managing the brokers themselves). One compose file, 3 brokers, durability triplet preconfigured.
- Pre-class: `docker compose up -d` → wait for healthy → `uv run python src/create_topics.py`.
- Brokers on the laptop: **localhost:19092 / 29092 / 39092**. Client = **`confluent-kafka`** (librdkafka wrapper), NOT `kafka-python` (unmaintained).
- **⚠ Live gotcha (verified):** producing within a few seconds of `docker compose up` can print one line — `Failed to acquire idempotence PID … Coordinator load in progress: retrying`. **Harmless** — the transaction coordinator is still loading; it retries and delivers. (Pre-warm by producing once before class.)

### Slide 16 — Live: describe the topic. "Where do the *leaders* land?"
- Predict-first: 6 partitions, 3 brokers, RF=3 — guess the leader layout + what the ISR column shows on a healthy cluster.
- **Command:**
  ```bash
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
  ```
- **Slide's ideal:** leaders spread `1,2,3,1,2,3` — each broker leads 2, follows 4; ISR full = health.
- **⚠ Live gotcha (verified, important):** right after a cluster restart, **all leaders can pile on ONE broker** (I just saw every leader = broker 2). That's a *leadership-balance* artifact, not a health problem — ISR was full (`2,3,1`) the whole time. The pretty spread only appears after a **preferred-leader election** (auto-runs ~every 5 min; force it with:)
  ```bash
  docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh \
      --bootstrap-server kafka-1:9092 --election-type preferred --all-topic-partitions
  ```
  After forcing it: `P0→3 P1→1 P2→2 P3→3 P4→2 P5→1` (each broker leads exactly 2; leader = first replica = "preferred"). **Teaching gold:** the discrepancy itself is the lesson — leadership and durability are *different* properties; watch the ISR, not the leader spread.

### Slide 17 — The producer: "Produce, keyed. Confirm, async."
- `acks="all"` + `enable.idempotence=True` (the producer's half of the durability triplet; idempotence is the L2 of slide 12).
- `key=str(customer_id).encode()` → the key decides the partition.
- **The #1 beginner data-loss bug, say it twice:** `produce()` only **enqueues** into a client-side buffer. `poll()` drives the network + fires delivery callbacks; `flush()` drains before exit. Skip both → script ends with messages **still in the buffer, sent nowhere, no error shown.**
- Map it: `produce()` = "write it down," `poll()`/`flush()` = "actually mail it."

### Slide 18 — Live: keyed vs keyless. "50 keys, 6 partitions. *Even split?*"
- Predict-first: produce 1,000 keyed by `customer_id`, then 1,000 `--keyless`. Guess both distributions.
- **Commands (run in order, in the same terminal):**
  ```bash
  # Terminal 1: produce 1,000 keyed messages
  uv run python src/produce_orders.py --count 1000
  # Output: KEYED (key=customer_id) — LUMPY: P0:78  P1:198  P2:61  P3:109  P4:163  P5:391

  # Terminal 1: produce 1,000 keyless messages
  uv run python src/produce_orders.py --count 1000 --keyless
  # Output: KEYLESS (key=None) — EVEN:   P0:178 P1:152 P2:170 P3:157 P4:170 P5:173
  ```
- **Verified real output (yours will vary; the SHAPE is the point):**
  ```
  KEYED (key=customer_id) — LUMPY:     P0:78  P1:198  P2:61  P3:109  P4:163  P5:391
  KEYLESS (key=None)      — EVEN:      P0:178 P1:152 P2:170 P3:157 P4:170 P5:173
  ```
- The takeaway line: **"Hashing is not balancing."** 50 keys over 6 partitions lands lumpy, and a few heavy customers (the zipf weighting in the script) make it lumpier. Keyless spreads evenly but **gives up per-key order.** *You trade order for balance with the key field.* (This is slide 8 made empirical.)

### Slide 19 — The consumer: "The poll loop is the *heartbeat.*"
- `consumer.poll(timeout=1.0)` does **fetch + heartbeat in one call** — stop polling and the broker thinks you died (→ rebalance; see annex gotcha `max.poll.interval.ms`).
- `enable.auto.commit=False` ("WE own the watermark"); `auto.offset.reset=earliest` applies **only** on first run (no committed offsets yet).
- **process, THEN `commit(asynchronous=False)`** — commit-per-message is correct but SLOW (fine for learning; `consume_rebalance.py` batches like prod).

### Slide 20 — Live: "Kill it. Restart. *Zero re-reads.*"
- **Commands (run in order, in the same terminal):**
  ```bash
  # Terminal 1: start consumer (fresh group)
  uv run python src/consume_naive.py
  # Output: [cXXXX] resuming: P0@earliest P1@earliest ... (first run)
  #         [cXXXX] stopped after N messages. Last commit: P0@7062 ...

  # Terminal 1: Ctrl-C to stop, then restart with the same group
  uv run python src/consume_naive.py
  # Output: [cXXXX] resuming: P0@7063 P1@... (exactly one past = zero re-reads)
  ```
- The payoff of the whole spine. Run consumer → it reads + commits per message → **Ctrl-C** → **restart same group**.
- **Verified:** Run 1 (fresh group) starts `resuming: P0@earliest …`, processes msgs, last commit `P0@7062`. Run 2 (same group) prints `resuming: P0@7063 …` — **exactly one past**, i.e. **zero re-reads, zero bookkeeping code.** `__consumer_offsets` did the L4 `pipeline_metadata` job.
- **⚠ Demo-staging tips (learned live):**
  - The `orders` topic **accumulates across every prior run**, so a fresh group has a big backlog and commit-per-message won't reach the end in a few seconds — you'll demo *resume-from-committed mid-stream*, which still makes the point. For a crisp "restart → reads NOTHING" finish, produce a small bounded batch into a **fresh topic/group** and let it fully drain first.
  - Use a **fresh `--group` name** each class run for a deterministic starting point.
  - In a real terminal, **Ctrl-C is clean** (SIGINT straight to the foreground process) — the `consumer.close()` leaves the group tidily. (Scripted SIGINT through `uv run` can orphan the child; not a concern when you Ctrl-C by hand.)
  - Then: start a **second** consumer in the same group → watch partitions split → that's the rebalance → **Part two begins.**

### Timing: ~18–22 min (it's live). Budget for the two predict-first reveals (16, 18) and the restart payoff (20).

### Transition line → Movement 5 ("Now break it")
> "A consumer that works alone is the easy part. The real lesson is what happens when the group *changes* — a member joins, one dies, the lag climbs, events arrive out of order, a broker disappears. Let's break it on purpose."

# MOVEMENT 5 — Now break it (slides 21–27)

**The one thing to land:** *A consumer that works alone is trivial. Production is the GROUP changing under you — joins, deaths, lag, disorder, dead brokers. At-least-once means a few duplicates are normal; correctness is "nothing LOST + an idempotent sink." This is the take-home.*



> ⚠ **Read this before teaching live — three places reality diverges from the slides. All verified on this cluster.** I fixed one real bug (`watch_lag.py`); the other two are narrative gaps to pre-empt out loud.

### Slide 21 — Divider: "Now break it."
- The pitch: works-alone → survives-the-group. Five failure modes coming: join, death, lag, disorder, dead broker.

### Slide 22 — Rebalance callbacks: "Commit on the way *out.*"
- `on_revoke` → commit `consumer.position()` for the partitions being taken (the handoff note). `on_assign` → init per-partition state. `partition.assignment.strategy=cooperative-sticky`.
- This is L5's apply-then-confirm meeting a new failure mode: work isn't lost to a crash, it's **reassigned**. Code lives in `consume_rebalance.py` (note the `COMMIT_RACES` guard — a commit that races a rebalance is *refused*, and crashing on that would be the real bug).

### Slide 23 — LIVE rebalance experiment ⚠ **biggest reality gap**
- **Commands (run in order, in separate terminals):**
  ```bash
  # Terminal 1: producer (to keep data flowing)
  uv run python src/produce_orders.py --rate 100

  # Terminal 2: consumer A (starts alone, owns all 6 partitions)
  uv run python src/consume_rebalance.py --name A
  # Output: [cXXXX] ASSIGNED  +6: P0@... P1@... P2@... P3@... P4@... P5@...

  # Terminal 3: consumer B (joins the same group, triggers rebalance 1)
  uv run python src/consume_rebalance.py --name B
  # Output: [cYYYY] ASSIGNED  +3: P3@... P4@... P5@...
  #          [cXXXX] REVOKED  -3: P3,P4,P5 -> committed P3@... P4@... P5@...

  # Now SIGKILL consumer B (in terminal 3): kill -9 <pid> or Ctrl-Z then kill %1
  # Terminal 2 will show: [cXXXX] REVOKED  +3: P3,P4,P5 -> committed P3@... P4@... P5@...
  #                          [cXXXX] ASSIGNED  +3: P3@... P4@... P5@...
  # And at the end: duplicates: ~83, lost: 0

  # Repeat with --skip-revoke-commit on consumer B to see the broken version:
  uv run python src/consume_rebalance.py --name B --skip-revoke-commit
  # Then SIGKILL B and watch duplicates climb to ~101, lost: 0
  ```
- **What to teach from the output:**
  - **ASSIGNED:** Consumer got these partitions at these offsets
  - **REVOKED:** Consumer is losing these partitions; it commits current positions before handoff
  - **The duplicate count:** Printed at the end when a consumer exits. Correct ~83, broken ~101, both lost=0
- **The key insight:** Even with correct callbacks, you get ~83 duplicates because B is SIGKILLed and can't commit its in-flight work. Skipping the revoke commit adds ~18 more (the clean handoff's in-flight). The lesson: crashes make SOME duplicates unavoidable → idempotent sinks are the real defense.
- **Slide now reframed to match reality** (was a misleading "0 vs 212"): correct ≈ **~83 dup**, broken ≈ **~101 dup**, **both `lost = 0`.** Numbers vary per run; the margin is modest by design — say so.
- **Why** (this IS the deeper lesson, and it's now on the slide): the experiment **SIGKILLs B**, and a hard-killed consumer can't run `on_revoke` — so ~80 duplicates from B's uncommitted in-flight work appear in *both* runs (unavoidable at-least-once). `--skip-revoke-commit` only adds the in-flight window on the *clean* revoke (rebalance 1), bounded by `COMMIT_EVERY=100` → the modest extra (~18).
- **What to teach from it:** (1) `lost = 0` ALWAYS — at-least-once held both times; (2) skipping the revoke commit measurably increases duplicates; (3) a crash makes *some* duplicates unavoidable → **the only real defense is an idempotent sink** (L4/L5). A *better* takeaway than a fake "0".

### Slide 24 — Lag: "the same number, *third costume.*"
- `lo, hi = consumer.get_watermark_offsets(tp)`; `lag = hi - committed_offset`, per partition. Mirror of L5: `pg_current_wal_lsn() − confirmed_flush_lsn`.
- Flat lag = keeping pace; growing = falling behind. **Nobody's disk fills when you fall behind** (retention by time) — that's what you bought leaving the slot model.

### Slide 25 — LIVE lag under a slow consumer ⚠ **fixed a real bug**
- **Commands (run in order, in separate terminals):**
  ```bash
  # Terminal 1: producer at full speed
  uv run python src/produce_orders.py --rate 100

  # Terminal 2: slow consumer (sleeps 10ms per message)
  uv run python src/consume_rebalance.py --name A --slow 10

  # Terminal 3: lag reporter
  uv run python src/watch_lag.py --group order-processor
  # Output: consumers   total lag    trend
  #         1           12,406       climbing ~900/s
  #         1           21,512       climbing

  # Terminal 4: add a second consumer to the group
  uv run python src/consume_rebalance.py --name B --slow 10
  # Terminal 3 will show: 2 (joined)  23,108       flattening…
  #                    2           22,887       draining ~450/s
  ```
- **Bug I found & fixed:** `watch_lag.py` was calling `probe.committed()`, which returns the **probe's own group** (`<group>-probe`, never commits) — so it always read committed=0 and reported the **entire backlog** as lag; the "draining" payoff would never show. **Fixed** to query the real group via `admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, None)])`. Verified against `kafka-consumer-groups.sh --describe` (P4 committed=12601, lag=10,046 — exact match).
- **Live staging note:** a fresh group starts at `earliest`, so initial lag = the **whole accumulated topic** (I saw ~120k). It's real, just dominated by backlog. For a clean "climb then drain" curve, use a fresh topic or a group already caught up, then throttle the consumer with `--slow`.
- **Key question to leave them with:** lag still grows with 6 consumers on 6 partitions — options? *(None free: repartition (rebalance + key reshuffle), or make each consumer faster. The partition count you chose day one is the ceiling.)*

### Slide 26 — Out-of-order: "Kafka keeps *produce* order. Not *event* order."
- **Commands (run in order):**
  ```bash
  # Terminal 1: produce shuffled lifecycle events (created → paid → shipped in random order)
  uv run python src/produce_out_of_order.py

  # Terminal 1 (same terminal): read back and see the disorder
  uv run python src/produce_out_of_order.py --readback
  # Output: partition 1:
  #           offset 0  order 4  paid     ts=16:14:05
  #           offset 1  order 4  shipped  ts=16:14:30
  #           offset 2  order 3  paid     ts=16:14:05   <- timestamp disorder
  #           offset 3  order 3  created  ts=16:14:00   <- preserved faithfully
  ```
- **Verified output is clean and on-message:**
  ```
  partition 1:
    offset 0  order 4  paid     ts=16:14:05
    offset 1  order 4  shipped  ts=16:14:30
    offset 2  order 3  paid     ts=16:14:05   <- timestamp disorder, preserved faithfully
    ...
    offset 6  order 3  created  ts=16:14:00   <- timestamp disorder, preserved faithfully
  ```
- The log records **arrival**, faithfully — including faithfully *wrong* timestamp order. If logic needs event-time order, Kafka alone can't give it → **watermarks, Lesson 7.** This shuffled topic is next week's lab rat. (Solid demo — no caveats.)

### Slide 27 — LIVE kill the brokers ⚠ **second reality gap**
- **Commands (run in order, in separate terminals):**
  ```bash
  # Terminal 1: producer with short timeout so failures are visible
  uv run python src/produce_orders.py --rate 50 --message-timeout 10000
  # Output: producing... (continuous)

  # Terminal 2: kill one broker
  docker stop kafka-3
  # Terminal 1: producer continues uninterrupted (ISR 3→2, still ≥ min.insync=2)

  # Terminal 2: kill a second broker
  docker stop kafka-2
  # Terminal 1: producer starts printing FAILED: …_MSG_TIMED_OUT after ~10s
  # (ISR 2→1, below min.insync=2 → writes refused)

  # Terminal 2: check broker logs for the real reason
  docker logs kafka-1 | grep NotEnough
  # Output: NotEnoughReplicasException: ISR Set(1) is insufficient to satisfy
  #         the min.isr requirement of 2 for partition orders-3

  # Terminal 2: restart brokers
  docker start kafka-2 kafka-3
  # Terminal 1: producer resumes (ISR heals to 1,2,3)
  ```
- **Slide claims:** `docker stop kafka-3` → writes continue (ISR shrinks); `docker stop kafka-2` → **producer prints `NOT_ENOUGH_REPLICAS`**; restart → resumes.
- **Verified reality:**
  - Stop ONE broker: ISR 3→2, still ≥ `min.insync=2` → **writes flow.** ✓ (matches)
  - Stop the SECOND: writes do stop — but the **Python producer never prints `NOT_ENOUGH_REPLICAS`.** With the real config (`enable.idempotence`, default `delivery.timeout.ms=300000`) it **silently retries for 5 minutes** — the delivery callback fires nothing. With a short `message.timeout.ms` it surfaces **`_MSG_TIMED_OUT`** ("Local: Message timed out"), still not `NOT_ENOUGH_REPLICAS`. (Verified split: 376 OK / 190 timed-out.)
  - The literal `NOT_ENOUGH_REPLICAS` IS real — it's in the **broker** log: `docker logs kafka-1 | grep NotEnough` → `NotEnoughReplicasException: ISR Set(1) is insufficient to satisfy the min.isr requirement of 2 for partition orders-3`.
  - **Topology confound:** these are combined **broker+controller** nodes (3 of them). Stopping 2 also **loses the KRaft controller quorum** (need 2 of 3) — so it's not a pure `min.insync` demo; the cluster loses its brain too.
- **Recommended live recipe (richer than the slide):** start the producer **with the new flag** so the failure is visible:
  ```bash
  uv run python src/produce_orders.py --rate 50 --message-timeout 10000
  ```
  then → `docker stop kafka-3` (producer uninterrupted; `--describe` shows Isr shrink to 2) → `docker stop kafka-2` (producer starts printing `FAILED: …_MSG_TIMED_OUT` after ~10s — **verified live**) → `docker logs kafka-1 | grep NotEnough` (reveal the *broker-side* reason: `NotEnoughReplicasException … min.isr requirement of 2`) → `docker start kafka-2 kafka-3` (Isr heals to 1,2,3) → producer resumes.
- **`--message-timeout MS` flag added to `produce_orders.py`** (default off = librdkafka's 5-min `delivery.timeout.ms`). Without it the producer just goes silent on the 2nd kill — which is *also* a fine demo if you prefer to show the silence and then the broker log.
- The principle still lands (and is the L2 callback): **the refusal is the feature** — Kafka won't accept data it can't replicate safely; same as CockroachDB refusing writes when the 2nd node died (dynamic ISR vs fixed quorum).

### Also note (post-restart, every time): leadership re-consolidates onto one broker (saw all leaders → broker 1 after the kill demo). Harmless; ISR is what matters. Force spread with the preferred-leader election (Movement 4, slide 16) if you want a pretty `--describe`.

### Timing: ~22–25 min (demo-heavy). The rebalance experiment (23) is the take-home's soul — budget the most time there, and pre-run it once so the ledger/timing is warm.

### Transition line → Movement 6 (the bridge)
> "Everything you just hand-rolled — the loop, the commits, the rebalance callbacks, the lag math — Spark Structured Streaming does for you next week. You built it once so its version is never a black box. Let's see the mapping."

# MOVEMENT 6 — Bridge to Spark + synthesis + take-home (slides 28–33)

**The one thing to land:** *Everything you hand-rolled today is what Spark automates next week — but the mechanics (offsets, rebalances, lag) are still how you'll debug it.* The "why did we do this?" question gets answered here.

### Slide 28 — Part three divider: "You'll never write a poll loop again"
- **Purpose:** Emotional transition from the grind of manual loops to the promise of automation.
- **Key message:** Everything you hand-rolled today (loop, commits, callbacks, lag math) → Spark does for you next week. You wrote it once so its version is never a black box.
- **Pedagogical framing:** This is the payoff for the pain of Movement 4. The "why did we do this?" question gets answered here.
- **Connection to L4/L5:** Same pattern: you built it → Airflow/Debezium ran it. Now: you built it → Spark runs it.

### Slide 29 — The mapping table (manual → Spark)
- **Table rows:**
  - `consumer.poll()` loop → Micro-batch scheduler polls on each trigger
  - Manual commits + `on_revoke` → Offsets written to checkpoint dir after each batch
  - `on_assign` / `on_revoke` callbacks → Partition assignment managed internally
  - `get_watermark_offsets()` lag math → `StreamingQueryProgress` rates & offset ranges
  - `json.loads` per message → `from_json()` with declared schema
  - **Keying & partition reasoning** → Still yours (no framework chooses your keys)
- **Pedagogical note:** Highlight the last row — that's the part Spark can't automate. Keys are architecture, always.
- **Debugging angle:** When Spark throws "streaming query failed," you'll debug at THIS level (offsets, rebalances, lag). Today's work is diagnostic infrastructure.

### Slide 30 — Spark preview code
- **Code shown:** The L7 `readStream` / `writeStream` pipeline, shown early on purpose.
- **Key lines to point out:**
  - `format("kafka")` → your poll loop
  - `startingOffsets="earliest"` → `auto.offset.reset`
  - `from_json(..., schema)` → your `json.loads`
  - `checkpointLocation="/ckpt"` → your commits (the interesting line)
  - `trigger(processingTime="10 seconds")` → your `while True`
- **Pedagogical note:** The `checkpointLocation` is the magic: Spark stores offsets AND processing state atomically → that's how it upgrades at-least-once to exactly-once. The price: L8.
- **Why show it now:** Demystify. It's not magic — it's the same parts, scheduled.

### Slide 31 — Synthesis: One structure, four rules
- **Core message:** Kafka is an append-only log with consumer-owned positions. Everything else is a corollary.
- **Four rules to carry out of the room:**
  1. **Process, then commit.** The watermark rule, third costume. Pair with idempotent sink → replay is free.
  2. **Keys are architecture.** They decide ordering, balance, and who suffers when a customer goes viral. Decide before the first message.
  3. **Parallelism = partitions.** Chosen at creation, expensive to change. Capacity-plan like shards.
  4. **Lag is the metric.** Flat = healthy. Growing = scale consumers or accept delay. Nobody's disk fills anymore (the upgrade from slots).
- **Pedagogical note:** This is the "exam cheat sheet" moment. If they remember these four, they get Kafka.

### Slide 32 — What's next → L7
- **Setup:** Events flow through Kafka in real time, but "revenue in the last 5 minutes" is still unanswerable.
- **The hard part:** Break 03 proved events arrive out of order → "when is a window done?"
- **Architecture diagram:** TODAY (events flow, ordered per key, replayable) → needs (windows + event time) → LESSON 7 (Spark Structured Streaming, tumbling windows, watermarks).
- **Call to action:** "Bring your out-of-order topic. It's the lab rat."
- **Pedagogical framing:** This is the cliffhanger. The problem they saw today (out-of-order) is the problem L7 solves.

### Slide 33 — Take-home assignment
- **Deliverable:** Ship a consumer that survives the rebalance.
- **Requirements:**
  - Producer keyed by `customer_id`
  - Consumer with manual commits (no auto-commit), cooperative-sticky assignment
  - `on_revoke` commits in-flight work; `on_assign` initializes per-partition state
  - Lag reporter printing per-partition and total lag on an interval
  - Proof: script or README walkthrough of join → kill → rejoin showing rebalance events, commits, and zero loss
- **Grading standard:** Grader starts consumer, adds second, kills it, reads lag output. Repo with README (your words: what callbacks do, what broker kill printed) + AGENTS.md/CLAUDE.md. AI-assisted is fine — grade is on rebalance being *correct*.
- **Pedagogical note:** This is the "you can do this" moment. They've seen the demo; now they ship it. The README requirement forces them to articulate the mechanics (teaching by explaining).

### Timing: ~15–18 min (conceptual, no demos). This is the "pull it together" movement — don't rush the synthesis slide (31). The four rules are the exam cheat sheet.

### Transition line → Movement 7 (annex)
> "If you want to re-run these demos at home, everything you need is in the annex — commands, prompts for AI assistance, and the gotchas that will bite you. Let's walk through it."

---

# MOVEMENT 7 — Annex / reference (slides 34–38)

**The one thing to land:** *Here's your recipe for re-running the demos and debugging the inevitable gotchas.* This is the "at home" reference.

### Slide 34 — Annex: Run it yourself
- **Commands:**
  ```bash
  cd src-lesson6
  docker compose up -d                          # 3 brokers, KRaft
  uv run python src/create_topics.py            # orders: 6 partitions, RF=3
  uv run python src/create_topics.py --describe   # sanity: ISR shows all 3 per partition
  uv run python src/produce_orders.py --rate 100 # keyed events, continuous
  uv run python src/consume_rebalance.py        # terminal 2 (and 3: same group)
  uv run python src/watch_lag.py                # the only number that matters
  uv run python src/produce_out_of_order.py     # break 03: L7's lab rat
  docker stop kafka-3                           # break 04 (then kafka-2 → refused)
  ```
- **Reset:** `docker compose down -v` wipes broker volumes (and stale cluster IDs). Consumer groups reset by changing `group.id` or `kafka-consumer-groups.sh --reset-offsets`.
- **Pedagogical note:** This is the "at home" reference. If they want to re-run the demos, they have the recipe.

### Slide 35 — Annex: OpenCode prompts
- **Purpose:** Show how to use AI assistance effectively for this pipeline.
- **Prompts:**
  - **Producer:** "Write a confluent-kafka producer sending order JSON to 'orders', keyed by customer_id, acks=all, idempotence on, printing partition+offset from the delivery callback. Don't forget poll(0) and flush()."
  - **Rebalance:** "Add on_assign/on_revoke with cooperative-sticky to my consumer. on_revoke must synchronously commit consumer.position() for the revoked partitions BEFORE they're taken."
  - **Lag:** "Write watch_lag.py: every 5s print per-partition lag (high watermark via get_watermark_offsets minus committed offset) and the total, for group 'order-processor'."
- **Pedagogical framing:** Let AI draft the mechanical parts; you own the correctness decisions (commit ordering, revoke callback, key choice). This is the "AI as junior engineer" model.

### Slide 36 — Annex: Architecture diagram
- **Diagram:** Producers → Kafka (3 brokers, KRaft, topic orders RF=3 min.isr=2) → Group "order-processor"
  - P0 leader kafka-1 → consumer A
  - P1 leader kafka-2 → consumer A
  - P2 leader kafka-3 → consumer A
  - P3 leader kafka-1 → consumer B
  - P4 leader kafka-2 → consumer B
  - P5 leader kafka-3 → consumer B
  - lag = distance between high watermark and committed offset
- **Callout:** Add a second *group* (different `group.id`) → gets the whole stream again, independently. The fan-out that cost one replication slot per reader in L5 is free here.
- **Pedagogical note:** This is the mental model anchor. If they can draw this, they understand Kafka.

### Slide 37 — Annex: Gotchas
- **Gotchas:**
  1. **Advertised listeners:** Broker replies "talk to me at this address" — if unreachable from laptop, you connect then mysteriously timeout. That's why each broker advertises its own port (19092/29092/39092). #1 Docker-Kafka failure.
  2. **`produce()` is buffered:** No `poll()`/`flush()` = no delivery, no error, no trace. Always flush before exit.
  3. **Stale volumes break cluster formation:** Recreated compose file? Old volume keeps old cluster ID, brokers refuse to join. `docker compose down -v`.
  4. **Slow processing = rebalance storm:** Exceed `max.poll.interval.ms` between polls → evicted → group rebalances twice. Do heavy work in batches, poll often.
  5. **`confluent-kafka`, not `kafka-python`:** The latter is pure-Python, slow, and unmaintained. Use the C-backed librdkafka binding.
- **Pedagogical note:** This is the "read before you debug" slide. If they hit these, they're not alone.

### Slide 38 — (presumably end slide)
- **Purpose:** Thank you / Q&A / next steps.
- **Pedagogical note:** If there's an end slide, remind them of the take-home and the L7 cliffhanger.

### Timing: ~5–8 min (reference, can be skimmed or skipped if time is tight). This is the "if you need it later" material.

---

## Final notes

- **The spine:** If they remember "this is your third log" and "process, then commit," you won.
- **The take-home:** The rebalance demo (slide 23) is the soul of the assignment. If they nail the revoke callback and idempotent sink, they get Kafka.
- **The cliffhanger:** Out-of-order events (slide 26) → L7 watermarks. Make sure they keep that topic around.
