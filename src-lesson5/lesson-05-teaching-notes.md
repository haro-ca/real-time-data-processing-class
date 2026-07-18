# Lesson 5 — Change Data Capture · DETAILED TEACHING SCRIPT

> **How to use this:** Each slide has **SAY** (you can read it aloud), **DO** (keys to press),
> **EXPECT** (what the screen shows so you know it worked), and **→ NEXT** (the sentence that
> bridges to the next slide). Demos also have **WHILE IT RUNS** (talk track for dead air) and
> **IF IT BREAKS** (recovery).
>
> ### ⭐ The one rule for keeping pace
> If a command misbehaves and you can't fix it in ~60 seconds: **read the EXPECT block out loud
> as if it happened, say "you'll reproduce this in the lab," and move on.** The narrative is the
> lesson; the live output is just proof. Never debug in front of the room for more than a minute.
>
> ### The 4 ideas the whole class hangs on (if you remember nothing else)
> 1. Polling asks "what changed?" — the table **can't answer honestly** (deletes vanish, clocks lie).
> 2. The fix isn't a better query, it's **reversing the direction**: the DB *pushes* changes (CDC).
> 3. The push stream is just the **WAL** you've had since Lesson 1, decoded back into rows.
> 4. **Apply, THEN confirm.** Every failure mode (crash, snapshot, drift, disk-full) comes back to this.

---

## 🖥️ TERMINAL SETUP (do this before class)

Open **two terminals**, both `cd src-lesson5`:

- **T1 — psql** (you'll type SQL here). Start it now:
  ```bash
  docker compose exec postgres psql -U bench -d bench
  ```
- **T2 — scripts** (you'll run `uv run python ...` here).

Open the deck in the browser. **Press `R`** on a slide to un-blur a spoiler.

---

## ✅ PRE-FLIGHT (60 seconds before you start talking)

In **T2**:
```bash
docker compose ps          # both lesson5-postgres and lesson5-runner = Up/healthy
docker compose exec -T postgres psql -U bench -d bench -tAc "SELECT count(*) FROM orders;"
```
**EXPECT:** `1000000`, no replication slot yet, no `data/cdc.duckdb`. If the count is wrong or you want a guaranteed-clean start, run the **RESET** block (bottom of this doc) — it takes ~10 s.

---

## 📋 COMMAND CHEAT-SHEET (the whole class, in order — glance here if lost)

| When | Terminal | Command |
|---|---|---|
| s4 | T2 | `uv run python src/poll_sync.py --audit` |
| s14 | T2 | `uv run python src/setup_cdc.py` |
| s15 | T1 | `UPDATE orders SET status='shipped',updated_at=now() WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 1);` |
| s15 | T1 | `DELETE FROM orders WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 1 OFFSET 5);` |
| s15 | T1 | `SELECT data FROM pg_logical_slot_peek_changes('orders_slot',NULL,NULL,'format-version','2','add-tables','public.orders');` |
| s16-17 | T2 | `uv run python src/cdc_consumer.py --once` |
| s18-19 | T2 | `uv run python src/watch_lag.py --iters 3` |
| s21 | T1 | `SELECT count(*) FROM orders;`  → compare with mirror (consumer's last "mirror = N rows" line) |
| s22 | T1 | `INSERT INTO orders (customer_id,amount,status) VALUES (1,99.00,'pending');` |
| s22 | T2 | `uv run python src/snapshot.py` then `uv run python src/cdc_consumer.py --once` |
| s23 | T1 | `UPDATE orders SET status='delivered',updated_at=now() WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 30);` |
| s23 | T2 | `uv run python src/cdc_consumer.py --crash-after 20` then `--once` then `--once` again |
| s24b | T2 | `uv run python src/experiment_schema_drift.py` |
| s25 | T2 | `uv run python src/experiment_abandon_slot.py`  ← **RUN LAST** (adds 250k rows) |

**Mirror-count one-liner** (paste in T2 whenever you need it):
```bash
uv run python -c "import duckdb;print('mirror rows:',duckdb.connect('data/cdc.duckdb').execute('SELECT count(*) FROM orders').fetchone()[0])"
```

---
---

# ⏱️ 0:00 — BLOCK 0 · THE HOOK (slides 1–6, target 15 min)

## [s1] Title — "Capture the change."
**SAY:** "Last week we built a batch pipeline that's *correct* — and up to a day stale. Today we kill the staleness without giving up the correctness. The trick is to stop *asking* the database what changed and let it *tell* us."
**→ NEXT:** "First, let's remember exactly what we built and where it hurts."

## [s2] Recap L4 — "Correct. And a day behind."
**SAY:** "Our L4 job runs at 2 AM. It's idempotent, recoverable, schema-checked — trustworthy numbers. But between runs, the warehouse knows *nothing*. A row can be 24 hours stale. Same source, same target today — Postgres to DuckDB — we change **only the bridge**: instead of re-reading the table on a timer, we subscribe to its changes."
**→ NEXT:** "So what's the obvious first attempt? Everybody writes the same loop."

## [s3] The naive sync (polling)
**SAY:** "Don't re-read a million rows every night — add an `updated_at` column, remember the last time you synced, and pull only rows newer than that. Loop every few seconds. This is **polling CDC**, and it's where everyone starts. Show of hands — who's written almost exactly this?"
**SAY:** "It looks airtight. Let's run it against a realistic write mix and watch it lie."
**→ NEXT:** (go to s4)

## [s4] 🔴 LIVE — polling lies  ·  *predict-first*
**SAY (before running):** "Predict for me: I'm going to insert some rows, update some, delete some — then sync and compare. **Name the ways the copy will be wrong.**" (Take 2–3 guesses. Most get "stale"; few get "deletes".)
**DO (T2):**
```bash
uv run python src/poll_sync.py --audit
```
**WHILE IT RUNS (~3 s):** "It syncs the whole table first, then I inject 8 inserts, 6 updates that bump `updated_at`, 5 updates that *forget* to, and 12 deletes — then poll again and audit."
**EXPECT (read the bottom block aloud):**
```
rows                               999,996   1,000,008
ghost rows (deleted, still in copy)                  12
stale rows (status differs, missed UPDATE)            5
DRIFT: copy disagrees with source on 17 rows. No error was raised.
```
**SAY (the punchline — slow down here):** "Look at the last line. **Nothing crashed.** The copy is wrong by 17 rows and the program is *happy*. You find out when finance asks why the dashboard is off. *That* is the danger — not that it's wrong, but that it's wrong **silently**."
> ⚠️ Your numbers (12 ghosts, 5 stale) differ from the slide's stylized "−4 / 3". Just say "exact counts vary."
**IF IT BREAKS:** Read the EXPECT block as fact. The takeaway is "silent drift," not the digits.
**→ NEXT:** "You might think these are three bugs to patch. They're not. They're one impossible request."

## [s5] Why it can't be fixed — the diagnosis
**SAY:** "`What changed since time T?` is a question a table physically cannot answer honestly. **One:** a deleted row isn't in any SELECT — to poll deletes you'd add soft-deletes everywhere and warp your production schema to serve sync. **Two:** commit order isn't `updated_at` order — a transaction that starts before your poll but commits after it is invisible. **Three:** poll often and you tax production; poll rarely and you're stale. There's no good setting on that dial." 
**SAY:** "Patching each symptom just moves the lie around. The fix is a **different direction of information flow**."
**→ NEXT:** "Here's the inversion." (advance to the dark slide)

## [s6] The inversion (PULL → PUSH)  · *thesis slide*
**SAY (let it breathe):** "Polling is **pull** — you interrogate the table on a timer. Log-based CDC is **push** — the database hands you a continuous, ordered stream of every committed change, deletes included, each exactly once. **One inversion, and every symptom we just saw stops existing.**" (Remember this line — you'll close the lesson on it at s31.)
**→ NEXT:** "And the beautiful part: you already own this stream. You've had it since Lesson 1."

*(Checkpoint: you should be ~15 min in.)*

---

# ⏱️ 0:15 — BLOCK 1 · BUILD THE CONSUMER BY HAND (slides 7–19, target 30 min)

## [s7] The WAL was a log all along  · *L1 callback*
**SAY:** "Remember the write-ahead log from Lesson 1? Every committed transaction writes its changes to the WAL *before* Postgres tells the client 'done.' We met it as a **durability** mechanism. But think about what it actually is: a complete, ordered record of every change to every row. **That's an event stream hiding in plain sight.** CDC doesn't add anything to Postgres — it reads the log that was always there."
**→ NEXT:** "There's one switch that decides whether we can read it as *rows*."

## [s8] wal_level: minimal / replica / logical
**SAY:** "Three levels. `minimal` = just enough to crash-recover. `replica` = page-level bytes, literally 'write these bytes at this offset on disk' — that's **below** the level of tables and rows, so you can't reconstruct 'status went pending → shipped' from it. `logical` = table, operation, and column values, before and after. **Logical is the only one that decodes into changes.** Ours is already set to `logical` in the Compose file."
**→ NEXT:** "To actually read it, we need three pieces."

## [s9] Architecture — slot · plugin · publication
**SAY:** "**Replication slot** — a durable, named cursor into the WAL. Postgres won't recycle WAL the slot hasn't consumed, so you can disconnect and resume with zero loss. *(Plant this:)* the flip side — a slot nobody reads keeps WAL forever and fills the disk. Hold that thought. **Output plugin** — turns raw WAL into a readable shape; we'll use `wal2json`. **Publication** — declares which tables are in the stream; everything else is filtered out."
**→ NEXT:** "One setting decides whether you also see the *old* values."

## [s10] Replica identity FULL  *(cuttable if behind)*
**SAY:** "By default an UPDATE/DELETE event carries only the **primary key** of the old row — you learn *which* row and its new values, not what it used to be. Maintaining a materialized view? New values are enough. Auditing, or need the before-image? `REPLICA IDENTITY FULL` — at the cost of fatter WAL. We set FULL to keep the demo honest."
**→ NEXT:** "Last setup question: what format do we read the stream in?"

## [s11] pgoutput vs wal2json
**SAY:** "`pgoutput` is the built-in binary protocol — it's what Debezium uses, and reading it means unpacking bytes by offset. `wal2json` gives you plain JSON: `{action:'U', table:'orders', identity:{id:42}, columns:[...]}`. We read JSON **on purpose** — the byte format doesn't transfer anywhere, but the concepts (slot, LSN, lag, failure modes) transfer to *every* CDC system. When someone asks 'but real tools use pgoutput' — yes, and we'll see exactly that at the end."
**→ NEXT:** (advance through the s12 divider) "No Debezium yet. Let's build the consumer with our own hands so the production tool is never a black box."

## [s13] Setup — same box, one flag flipped
**SAY:** "Same Postgres, `wal_level=logical` already on, `wal2json` bundled in the image. The table's seeded with the same million orders from L4. Let's open the cursor into the WAL."

## [s14] 🔴 LIVE — create the slot
**DO (T2):**
```bash
uv run python src/setup_cdc.py
```
**EXPECT:**
```
REPLICA IDENTITY FULL  set ...
PUBLICATION orders_pub  -> public.orders
SLOT orders_slot  created with plugin=wal2json
consistent_point = 0/XXXXXXXX   <- stream starts AFTER this LSN
```
**SAY:** "That `consistent_point` LSN is a position in the WAL. **Everything after it** flows through the slot; everything before — our existing million — does **not**. Remember that; it's a trap we spring on ourselves in 20 minutes."
**IF IT BREAKS** (`slot already exists`): run `uv run python src/setup_cdc.py --reset`.
**→ NEXT:** "Let's prove the stream is real. I'll change a row and a delete, and we'll watch them appear."

## [s15] 🔴 LIVE — tail the slot (the money shot)
**DO (T1 — psql):**
```sql
UPDATE orders SET status='shipped', updated_at=now() WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 1);
DELETE FROM orders WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 1 OFFSET 5);
```
**DO (T1 — peek the slot, this does NOT advance it):**
```sql
SELECT data FROM pg_logical_slot_peek_changes('orders_slot', NULL, NULL,
        'format-version','2','add-tables','public.orders');
```
**EXPECT:** several JSON rows — a `"action":"B"` (begin), your `"action":"U"`, your `"action":"D"`, a `"action":"C"` (commit).
**SAY (point at the D):** "There it is — the **DELETE** that polling literally could not see arrives as a first-class `\"action\":\"D\"`. No `updated_at`, no soft-deletes, no clock math. And notice the begin/commit wrappers — that `C` carries the commit LSN, which matters in two slides."
**IF IT BREAKS:** the consumer in the next step will print the same changes; skip the peek and move on.
**→ NEXT:** "Now — how do we write these into DuckDB so that replays can't corrupt it?"

## [s16] Apply to DuckDB — idempotent  · *L4 callback*
**SAY:** "For every event we do **delete-then-insert** keyed on the primary key — the exact idempotent move from Lesson 4. The delete runs even for an INSERT, so if a crash makes us replay an insert, it deletes the row it already wrote and rewrites it — a **no-op**, instead of crashing on a duplicate key. Why not a plain `UPDATE SET`? Because **any** CDC consumer can crash and replay. Idempotent apply is what makes replay harmless — the property that lets you sleep."
**→ NEXT:** "And the loop that drives it is three verbs."

## [s17] The loop — peek · apply · advance  · *the spine of the lesson*
**SAY:** "**Peek** the pending changes — advances nothing. **Apply** them to DuckDB. **Only then advance** the slot — that's us telling Postgres 'you can recycle WAL up to here.' The order is everything: advance *before* applying and a crash loses data; *never* advance and the slot fills the disk; and advance only to a **commit marker's** LSN, never mid-transaction. The rule for the whole lesson: **apply, then confirm.** *(Forward hook:)* Kafka makes you play this exact game next week — there it's called *committing your offset.*"
**DO (T2 — run the consumer once to apply the changes from s15):**
```bash
uv run python src/cdc_consumer.py --once
```
**EXPECT:** `+2 applied ... confirmed 0/XXXX ... mirror N rows ... checksum ...` then `Done.`
**SAY:** "It peeked, applied, advanced. The mirror now holds only what's streamed **since the slot was created** — keep that in your head for the next block."
**→ NEXT:** "How do we know it's keeping up? One number."

## [s18] 🔴 LIVE — lag  *(keep this simple)*
**DO (T2):**
```bash
uv run python src/watch_lag.py --iters 3
```
**EXPECT:** three rows showing `orders_slot`, a `retained` size, a `lag_bytes` number, and `active False  <- no consumer!` (because nothing is streaming right now).
**SAY:** "`lag_bytes` is the distance between what's committed and what we've confirmed. Flat means we're keeping pace; climbing means we're falling behind. *(Then reveal the slide's spoiler with `R`:)* under a thousand inserts a second this stays flat at a couple hundred KB — that single number is the whole operational story."
> 🟡 **Don't attempt the full live load test** (it needs a separate load generator + a background consumer in a 3rd terminal). Showing the metric once + revealing the slide is enough. Offer the live-load version as a lab exercise.
**→ NEXT:** (s19)

## [s19] Lag is the ONE metric
**SAY:** "This is the raw version of what every CDC tool dresses up — Debezium ships it as a JMX metric, Kafka Connect calls it *consumer lag*. Same distance: 'what's committed' minus 'what I've confirmed.' In production, **this is the number you alert on.**"
**→ NEXT:** (advance through s20 divider) "Our consumer works in the demo. Now let's break it the four ways production breaks it."

*(Checkpoint: you should be ~45 min in. Half the clock, and the hardest material is behind you.)*

---

# ⏱️ 0:45 — BLOCK 2 · NOW BREAK IT (slides 20–26, target 30 min — DO NOT CUT)

## [s21] 🔴 LIVE — Break 01: the missing million
**SAY:** "Our consumer is perfect. So count both sides."
**DO (T1):**
```sql
SELECT count(*) FROM orders;
```
**EXPECT:** ~`999996`. **Then point at the consumer's last line from s17** — `mirror = 2 rows` (or whatever it printed).
**SAY:** "Source: a million. Mirror: **two.** Our consumer isn't broken — the slot only ever sees the **future**. The million rows that existed *before* the `consistent_point` never flowed through. **A change stream is not a copy.**"
**→ NEXT:** "To build a full mirror we need a one-time snapshot of the past, stitched to the stream with no gap and no overlap."

## [s22] 🔴 LIVE — Break 01 fix: snapshot, then drain
**SAY:** "Watch the seam. I'll insert a row *now* — it lands in the slot. Then I snapshot, and because the snapshot runs after the insert, that same row is in **both** the snapshot and the stream. We don't fight that — the idempotent sink turns the overlap into a no-op."
**DO (T1):**
```sql
INSERT INTO orders (customer_id, amount, status) VALUES (1, 99.00, 'pending');
```
**DO (T2):**
```bash
uv run python src/snapshot.py
uv run python src/cdc_consumer.py --once
```
**EXPECT:** snapshot reports ~`999,997 rows`; the consumer then prints `+1 applied` but the **mirror count doesn't change** (the overlap re-applied as a no-op).
**SAY:** "That insert was in the snapshot *and* the stream. The consumer re-applied it as delete-then-insert — a no-op. **No gap, no double-count.** Debezium gets an exact no-overlap stitch over the replication protocol; we traded that ceremony for plain SQL plus idempotency and converged to the same mirror. On a big table the snapshot can run for hours — now you know what it's doing."
**IF IT BREAKS:** the headline is "snapshot the past + stream the future = full mirror." Say it and move on.
**→ NEXT:** "Second way production bites: the consumer dies mid-stream."

## [s23] 🔴 LIVE — Break 02: crash & replay  · *predict-first*
**SAY (before running):** "Predict: I apply 20 changes, then **kill the process before it confirms**, then restart. What's the mirror look like after? Lost data? Duplicates? Or fine?"
**DO (T1 — make 30 changes):**
```sql
UPDATE orders SET status='delivered', updated_at=now() WHERE id IN (SELECT id FROM orders ORDER BY id LIMIT 30);
```
**DO (T2 — crash before confirming):**
```bash
uv run python src/cdc_consumer.py --crash-after 20
```
**EXPECT:** `applied 20 events ... (simulated crash before confirm) ... slot NOT advanced` + a checksum.
**DO (T2 — restart, then run a 2nd time to prove stability):**
```bash
uv run python src/cdc_consumer.py --once
uv run python src/cdc_consumer.py --once
```
**EXPECT:** first restart applies all 30 (the 20 replays are no-ops); **both runs print the identical checksum.**
**SAY:** "Same checksum, twice. The crash advanced nothing, so on restart we re-saw the 20 we'd already applied — and idempotency made them no-ops. **CDC is at-least-once: you never lose events, you may re-see a few.** The idempotent sink is what makes that *safe* instead of *corrupting*. Getting to *exactly*-once when you *can't* lean on an idempotent sink — that's all of Lesson 8."
**→ NEXT:** "Third way: the data's *shape* changes underneath you."

## [s24] Schema evolution (concept)
**SAY:** "Upstream runs `ALTER TABLE orders ADD COLUMN notes`. The stream doesn't break — wal2json just starts emitting the new field. Now **your consumer** has to notice and decide: **adapt** (add the column to the target and keep going — the production choice), or **fail loud** (if silent divergence is worse than an outage). CDC is a **schema contract** between producer and consumer — the hardest operational problem in streaming, and the reason schema registries exist. We'll meet those in Lesson 11."
**→ NEXT:** "Let's watch our own consumer make the wrong choice first — then the right one."

## [s24b] 🔴 LIVE — Break 03: drift, silent then loud
**DO (T2):**
```bash
uv run python src/experiment_schema_drift.py
```
**WHILE IT RUNS:** "Act 1 runs with `--ignore-drift`; Act 2 runs strict, the default."
**EXPECT:**
```
ACT 1 ... consumer --ignore-drift exited 0 (no error raised)
   THE MIRROR LIES. ...
ACT 2 ... consumer (strict) exited 2:
   SCHEMA DRIFT — failing loud, on purpose.
   slot NOT advanced — zero events lost.
... source restored ... mirror converged.
```
**SAY:** "Act 1 — our consumer drops the new column **silently**. The *exact* lie polling told, except this time **we** wrote it. Act 2 — strict mode refuses, exits, and here's the key: **failing loud cost us nothing.** The slot only advances *after* apply, so the rejected event is still sitting there. Fix the mirror, rerun, it replays. **Confirm-after-apply is what makes 'stop' a safe answer to drift.**"
**→ NEXT:** "Fourth and last — the failure that takes down not your pipeline, but the **source database**."

## [s25] 🔴 LIVE — Break 04: disk-full from an abandoned slot  · **RUN LAST**
**SAY:** "Remember the slot that retains WAL until you confirm? Create one, never read it, and write to the source."
**DO (T2):**
```bash
uv run python src/experiment_abandon_slot.py
```
**EXPECT:** `retained WAL` climbing `12 MB → 24 → 35 → 47 → 59 MB`, then `dropped 'abandoned_slot'`.
**SAY:** "Postgres can't recycle WAL the slot hasn't confirmed, so it piles up — left under real load, until the disk fills and **the source stops accepting writes.** An unmonitored slot can take down your production database. The safety valve is `max_slot_wal_keep_size` — it drops a greedy slot to save the DB, at the cost of a forced re-snapshot."
> ⚠️ This injected 250k rows. If you plan to demo anything else, run the **RESET** block first.
**→ NEXT:** (s26)

## [s26] The rule
**SAY:** "One rule ties all four breaks together: **every slot needs a monitored consumer.** Alert when `confirmed_flush_lsn` falls behind. Cap `max_slot_wal_keep_size` so a stuck consumer can't sink the database. Keep apply idempotent so the inevitable replay is a no-op."
**→ NEXT:** (advance through s27 divider) "Here's the payoff: you didn't just build a demo. You built Debezium."

*(Checkpoint: ~1:15. Home stretch — the rest is slides, no live demos.)*

---

# ⏱️ 1:15 — BLOCK 3 · YOU BUILT DEBEZIUM (slides 27–33, target 15 min · slides only)

## [s28] Debezium — the whole consumer as config
**SAY:** "Debezium is a Kafka Connect source connector. You don't write the loop — you POST a JSON config and it creates the slot, snapshots, streams, and publishes to Kafka topics. Notice `plugin.name: pgoutput` — it parses the binary protocol so you never have to. Every field here is a decision you made by hand: which tables, which slot, where the snapshot lands."
*(No need to run the Kafka overlay — the slides carry it. Mention it's in `debezium/` if a student wants to try it.)*

## [s29] By-hand → Debezium (the mapping table)  · *the takeaway slide*
**SAY:** "Read this table top to bottom. Slot and consistent_point → managed slot and offset storage. `snapshot.py` → initial snapshot phase. `slot_advance` → offset commits. Delete+insert → idempotent delivery. Detect-ALTER → schema history + registry. `watch_lag.py` → the JMX metric you must alert on. **Nothing new — everything automated.** In production you'll run Debezium, not your loop. But its failure modes — snapshot time, slot lag, schema drift — are things you've now **felt**, not bullet points in a doc."
**→ NEXT:** (s30)

## [s30] Outbox teaser
**SAY:** "One more thing CDC unlocks. The classic trap: an app must write to the DB **and** publish an event; do them separately and one can fail. The **outbox pattern**: insert your row and an `outbox` row in **one local transaction**, and let CDC stream the outbox downstream. A distributed-consistency nightmare becomes a local transaction plus a CDC read. That's the backbone of the next two lessons."

## [s31] Synthesis — polling vs log-based  · *close on the thesis*
**SAY (slow, this is the close):** "Deletes: invisible → first-class. Freshness: stale → sub-second. Load: every poll queries → one slot reads the WAL. Correctness: drifts silently → exact and replay-safe. **We didn't fix the four polling symptoms one by one. We changed the direction of information flow — and they stopped existing.**"
**→ NEXT:** "But this new thing has its own wall."

## [s32] The new wall → L6 (Kafka)
**SAY:** "CDC is real-time and correct, but the stream is **welded to one Postgres.** Want a second consumer? Second slot, double the WAL load. A consumer stalls? The *source* hoards WAL toward disk-full. **The source can't be your buffer.** Next week: Kafka — the commit log as a service. The same append-only log idea, from L1's WAL to today's slot, now decoupled so producers and consumers stop holding each other hostage."

## [s33] Take-home
**SAY:** "Ship a CDC mirror you can trust: snapshot + stream with no gap or overlap; idempotent apply that survives a kill-and-restart with identical checksum; a lag readout; and a documented choice for a schema change. The grader runs your consumer, **kills it, restarts it, and diffs Postgres against DuckDB. Parity = pass.**"
**→ DONE.** (s34–38 are annex/reference — point students to them for the run commands, opencode prompts, architecture diagram, gotchas, and script list. Don't present them.)

*(Checkpoint: ~1:30. You're done.)*

---

## 🧯 PANIC RECOVERY (tape this to your monitor)

- **A command hangs or errors:** read the EXPECT block aloud, say "you'll repro this in the lab," move on. ≤60 s.
- **`slot does not exist`:** `uv run python src/setup_cdc.py` (or `--reset`).
- **Consumer applies 0 changes:** you made the change *before* the slot existed — that's literally the s21 lesson; use it as a teaching moment.
- **DuckDB "file is locked" / IO error:** another script is holding the mirror. Make sure only one `uv run` touches `data/cdc.duckdb` at a time.
- **Everything's a mess, you want a clean slate mid-class:** run RESET (below), ~10 s, restart from s14.

## 🔄 RESET (clean, class-ready baseline)
```bash
cd src-lesson5
docker compose exec -T postgres psql -U bench -d bench -c "SELECT pg_drop_replication_slot('orders_slot');"
rm -f data/cdc.duckdb
uv run python src/seed_data.py        # ~4 s → 1,000,000 orders, no slot, empty mirror
```

_Verified end-to-end (host `uv run`) on 2026-06-13 — all six demos green, run twice._
