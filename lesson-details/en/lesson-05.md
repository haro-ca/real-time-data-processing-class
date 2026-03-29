# Lesson 5, CDC: the bridge between OLTP and everything else

Lesson 3 ended with a duality: OLTP and OLAP optimize for opposite access patterns, and no single storage layout serves both. Lesson 4 showed the classical answer, batch ETL, scheduled jobs, extract-transform-load on a timer. It works, but it has a fundamental problem: **you're always asking "what changed since last time?"** and the answer is only as fresh as your last batch run. CDC flips this. Instead of polling for changes, you subscribe to them. The database tells you what happened, as it happens, in the order it happened. This is the architectural inflection point of the course.

## Hour 1, Theory: how CDC works and why it changes everything

### Module A, The polling problem

Start with the approach everyone tries first. You have an OLTP Postgres with an `orders` table and you need to keep a DuckDB analytical copy in sync. Naive approach:

```sql
SELECT * FROM orders WHERE updated_at > :last_sync_time;
```

This is polling-based CDC. It has at least four problems, and students need to understand all of them before they'll appreciate log-based CDC:

1. **You need a reliable `updated_at` column.** Every table, every write path, must set it correctly. One ORM misconfiguration, one raw SQL `UPDATE` that forgets the column, and your sync silently misses rows. In practice, this breaks within the first month of any real system.

2. **Deletes are invisible.** A `DELETE FROM orders WHERE id = 42` leaves no trace for your polling query. You need soft deletes (`deleted_at` column) everywhere, which means your OLTP schema is now shaped by your CDC needs. That's the tail wagging the dog.

3. **Clock skew and transaction visibility.** A transaction that started at T=100 but committed at T=105 might be invisible to a poll at T=103 that uses `updated_at > T=100`. You either miss it or you need complex high-watermark logic with safety margins, and even then there are edge cases with long-running transactions.

4. **Cost.** Every poll is a query against your OLTP database. If you poll every second across 50 tables, that's 50 queries per second that compete with production traffic. If you poll less frequently, your data is staler. There's no good tradeoff point.

**Key insight to drive home:** polling-based CDC is fundamentally pull-based. You're asking the database "what changed?" repeatedly. Log-based CDC is push-based. The database tells you "here's what changed" as a continuous stream. That inversion, from pull to push, eliminates all four problems above.

### Module B, The WAL as a change stream

Students already know the WAL from Lesson 1, it's the durability mechanism. Every committed transaction writes its changes to the WAL before acknowledging the client. Here's the insight they haven't made yet: **the WAL is already a complete, ordered log of every change to every row in the database.** It's not just a crash recovery mechanism, it's an event stream hiding in plain sight.

Postgres has three WAL levels, and students saw these in Lesson 1:

- `wal_level = minimal`, enough for crash recovery. No replication.
- `wal_level = replica`, enough for physical replication (byte-for-byte WAL shipping). This is what streaming replicas use.
- `wal_level = logical`, the WAL includes enough information to **decode changes at the logical level**: which table, which columns, old values, new values. This is what CDC needs.

The difference between `replica` and `logical` is crucial. Physical WAL records describe page-level modifications, "write these bytes at this offset in this page." You can't reconstruct "row 42 in the orders table had its status changed from 'pending' to 'shipped'" from physical WAL records because they're below the abstraction level of tables and rows. Logical decoding lifts the WAL back up to the logical level: table name, operation type (INSERT/UPDATE/DELETE), column values before and after.

Walk through the Postgres logical decoding architecture:

1. **Replication slot**, a named cursor into the WAL stream. Postgres guarantees it won't recycle WAL segments that the slot hasn't consumed yet. This is critical: it means the consumer can disconnect, reconnect later, and pick up exactly where it left off. No data loss. But it's also dangerous, if a consumer stops consuming and the slot retains WAL forever, disk fills up. Students will see this failure mode in the practical.

2. **Output plugin**, transforms the internal WAL records into a consumable format. Postgres ships with `pgoutput` (the native logical replication protocol) and the community provides `wal2json` (JSON), `test_decoding` (human-readable text), and others. `pgoutput` is the right choice for production because it's built-in, maintained by core Postgres, and is what Debezium uses under the hood.

3. **Publication**, defines which tables are included in the change stream. You can publish all tables (`FOR ALL TABLES`) or specific ones. Changes to unpublished tables are filtered out by the output plugin.

```sql
-- Set up logical replication (one-time)
ALTER SYSTEM SET wal_level = logical;
-- Restart Postgres after this change

-- Create a publication for the tables you want to track
CREATE PUBLICATION orders_pub FOR TABLE orders;

-- Create a replication slot using the pgoutput plugin
SELECT pg_create_logical_replication_slot('orders_slot', 'pgoutput');
```

After this setup, every INSERT, UPDATE, and DELETE on the `orders` table is captured in the replication slot. The consumer reads from the slot, processes events, and confirms consumption (advancing the slot position). Postgres then knows it can recycle those WAL segments.

**Important detail on REPLICA IDENTITY:** by default, Postgres only includes the primary key in the "before" image of UPDATE and DELETE events. If the consumer needs the old values of non-key columns (e.g., to know that `status` changed from 'pending' to 'shipped'), the table needs `REPLICA IDENTITY FULL`:

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

This makes the WAL entries larger (they now include all old column values), but it's necessary for most CDC use cases. Without it, your consumer sees "row with id=42 was updated, here are the new values" but doesn't know what the old values were. For maintaining a materialized view, you might not need old values. For auditing or event sourcing, you do. Make students think about which case they're in.

### Module C, The outbox pattern

Before moving to the practical, introduce a pattern that will come back in Lessons 6-7.

Problem: your application needs to both write to the database and publish an event to an external system (Kafka, a webhook, another service). If you do both operations, one can succeed and the other can fail, you get inconsistency. Classic distributed systems problem.

The outbox pattern solves this by leveraging CDC:

1. Application writes to the `orders` table and inserts a row into an `outbox` table within the **same database transaction**. Atomicity is guaranteed by the local transaction, no 2PC needed.

```sql
BEGIN;
INSERT INTO orders (customer_id, amount, status)
    VALUES (7, 99.99, 'pending');
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
    VALUES ('order', currval('orders_id_seq'), 'OrderCreated',
            '{"customer_id": 7, "amount": 99.99}');
COMMIT;
```

2. CDC captures changes to the `outbox` table and publishes them downstream.
3. After successful publication, the outbox row can be deleted (or the consumer just ignores already-processed events).

The elegance: you've turned a distributed consistency problem (write to DB + publish event) into a local transaction problem (write to DB) plus a CDC problem (read the change stream). Since CDC is reliable (slot-based, no data loss), the entire pipeline has exactly-once semantics without any distributed transaction protocol.

Students don't need to implement this today, but they need to understand it because it's the foundational pattern for Lessons 6-7 when Kafka enters the picture.

### Module D, Debezium: the 10,000-foot view

Don't teach Debezium in depth yet, that's the optional expansion at the end. But students need the mental model now so the practical makes sense in context.

Debezium is a set of Kafka Connect source connectors that do log-based CDC for Postgres, MySQL, MongoDB, SQL Server, and others. For Postgres, Debezium:

1. Creates a logical replication slot using `pgoutput`
2. Reads the change stream via the Postgres replication protocol
3. Converts each change into a structured event (JSON or Avro)
4. Publishes events to Kafka topics (one topic per table by default)

In production, Debezium is the standard answer. It handles snapshotting (initial load of existing data), schema evolution, offset tracking, and fault tolerance. It's battle-tested at scale.

**But here's why we're not starting with Debezium:** it's a black box if you don't understand what's happening underneath. Students who set up Debezium via Docker Compose and see events appear in Kafka have learned nothing about replication slots, WAL decoding, or output plugins. They've learned how to follow a tutorial. By building a CDC consumer from scratch with `psycopg3`, students understand every layer. Then when they see Debezium, they know exactly what it's automating.

---

## Hour 2, Practical: build a CDC consumer from scratch

### Setup (10 min)

Postgres in Docker, same as previous lessons but with one critical change: `wal_level = logical`. Provide a Docker Compose file or a `docker run` command that sets this:

```bash
docker run -d --name pg-cdc \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16 \
  -c wal_level=logical \
  -c max_replication_slots=4 \
  -c max_wal_senders=4
```

Students need `psycopg[binary]` (version 3) and `duckdb`:

```bash
pip install "psycopg[binary]" duckdb
```

Create the source schema, reuse the `orders` table from Lesson 1, because continuity matters:

```sql
CREATE TABLE orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE PUBLICATION orders_pub FOR TABLE orders;
```

On the DuckDB side, create the target table that the CDC consumer will maintain:

```python
import duckdb

duck = duckdb.connect("materialized.duckdb")
duck.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id BIGINT PRIMARY KEY,
        customer_id INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        _cdc_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
""")
```

The `_cdc_updated_at` column tracks when the CDC consumer last touched this row. It's not in the source, it's metadata about the replication process. This is a best practice students should internalize: always track replication metadata separately from source data.

### Phase 1, Consuming the replication stream with psycopg3 (25 min)

This is the core of the lesson. `psycopg3` has first-class support for Postgres logical replication via its `ReplicationConnection` and streaming replication cursor. The API is thin but powerful.

Walk through the code step by step. Don't hand students a finished script, build it up incrementally.

**Step 1: establish a replication connection.**

```python
import psycopg
from psycopg import sql

# Replication connections use a special connection parameter
conn = psycopg.connect(
    "host=localhost dbname=postgres user=postgres password=postgres",
    autocommit=True,  # required for replication connections
)
```

**Step 2: create the replication slot programmatically.**

```python
# Create a logical replication slot if it doesn't exist
cur = conn.cursor()
cur.execute(
    "SELECT pg_create_logical_replication_slot(%s, %s)",
    ("orders_slot", "pgoutput"),
)
print(cur.fetchone())  # (slot_name, consistent_point)
```

The `consistent_point` is the WAL LSN (Log Sequence Number), a position in the WAL stream. Everything after this LSN will be delivered through the slot. Everything before it (existing data) won't, that's the snapshotting problem Debezium solves, which students will handle manually in Phase 3.

**Step 3: start streaming changes.**

This is where `psycopg3`'s replication API shines. The key is using the `start_replication` method on a replication cursor:

```python
from psycopg.pq import ExecStatus

# Open a replication cursor
cur = conn.cursor()
cur.execute(
    "START_REPLICATION SLOT orders_slot LOGICAL 0/0"
    " (proto_version '1', publication_names 'orders_pub')"
)

# The connection is now in streaming replication mode.
# Messages arrive as CopyData messages on the wire.
```

However, raw `pgoutput` messages are a binary protocol, not trivial to parse by hand. This is intentional. Students should understand what the raw protocol looks like, but parsing it manually is not the learning objective. Instead, use the `psycopg3` logical replication stream consumer, which handles the protocol framing:

```python
import psycopg
from psycopg_c._psycopg import adapt  # noqa, internal, but stable for our purposes

def create_cdc_consumer():
    """
    Connect to Postgres and consume the logical replication stream.
    Yields decoded change events as dictionaries.
    """
    conn = psycopg.connect(
        "host=localhost dbname=postgres user=postgres password=postgres",
        replication=True,  # this is the key parameter
        autocommit=True,
    )

    # Create cursor for replication
    cur = conn.cursor()

    # Start replication, pgoutput sends structured messages
    cur.execute(
        "START_REPLICATION SLOT orders_slot LOGICAL 0/0"
        " (proto_version '2', publication_names 'orders_pub',"
        "  messages 'true')"
    )

    return conn, cur
```

At this point, stop and explain what's actually happening on the wire. Postgres is now streaming WAL changes in the `pgoutput` protocol format. Each message has a type byte:

- `B`, Begin (transaction start, with XID and timestamp)
- `R`, Relation (table metadata, OID, name, column definitions)
- `I`, Insert (new tuple data)
- `U`, Update (old tuple + new tuple, if REPLICA IDENTITY FULL)
- `D`, Delete (old tuple key or full row)
- `C`, Commit (transaction end, with LSN and timestamp)

Students must write a decoder for these messages. This is the hard part, and it's where the actual learning happens. Provide the wire format specification (it's in the Postgres docs under "Logical Replication Message Formats") and have them implement it.

Here's the skeleton, students fill in the parsing logic:

```python
import struct
from datetime import datetime, timezone, timedelta

# Postgres epoch is 2000-01-01, not 1970-01-01
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def parse_pgoutput_message(data: bytes) -> dict:
    """Parse a single pgoutput protocol message."""
    msg_type = chr(data[0])

    if msg_type == 'B':  # Begin
        # 8 bytes: final LSN of the transaction
        # 8 bytes: commit timestamp (microseconds since PG epoch)
        # 4 bytes: transaction XID
        lsn = struct.unpack_from('!Q', data, 1)[0]
        timestamp_us = struct.unpack_from('!q', data, 9)[0]
        xid = struct.unpack_from('!I', data, 17)[0]
        ts = PG_EPOCH + timedelta(microseconds=timestamp_us)
        return {'type': 'begin', 'lsn': lsn, 'timestamp': ts, 'xid': xid}

    elif msg_type == 'C':  # Commit
        # 1 byte: flags
        # 8 bytes: commit LSN
        # 8 bytes: end LSN
        # 8 bytes: commit timestamp
        flags = data[1]
        commit_lsn = struct.unpack_from('!Q', data, 2)[0]
        end_lsn = struct.unpack_from('!Q', data, 10)[0]
        timestamp_us = struct.unpack_from('!q', data, 18)[0]
        ts = PG_EPOCH + timedelta(microseconds=timestamp_us)
        return {'type': 'commit', 'commit_lsn': commit_lsn,
                'end_lsn': end_lsn, 'timestamp': ts}

    elif msg_type == 'R':  # Relation
        # Relation messages describe the schema of a table.
        # They're sent before the first DML message for that table
        # in each transaction (or when the schema changes).
        return parse_relation(data)

    elif msg_type == 'I':  # Insert
        return parse_insert(data)

    elif msg_type == 'U':  # Update
        return parse_update(data)

    elif msg_type == 'D':  # Delete
        return parse_delete(data)

    else:
        return {'type': 'unknown', 'msg_type': msg_type}
```

The Relation message parser is the most complex, it contains column names, type OIDs, and type modifiers. Students need this to interpret the tuple data in Insert/Update/Delete messages. Give them this one:

```python
def parse_relation(data: bytes) -> dict:
    """Parse a Relation message, table schema metadata."""
    offset = 1
    # 4 bytes: relation OID
    rel_id = struct.unpack_from('!I', data, offset)[0]
    offset += 4
    # Namespace (schema name), null-terminated string
    ns_end = data.index(0, offset)
    namespace = data[offset:ns_end].decode('utf-8')
    offset = ns_end + 1
    # Relation name, null-terminated string
    name_end = data.index(0, offset)
    rel_name = data[offset:name_end].decode('utf-8')
    offset = name_end + 1
    # 1 byte: replica identity setting
    replica_identity = data[offset]
    offset += 1
    # 2 bytes: number of columns
    n_cols = struct.unpack_from('!H', data, offset)[0]
    offset += 2

    columns = []
    for _ in range(n_cols):
        # 1 byte: flags (1 = part of the key)
        flags = data[offset]
        offset += 1
        # Column name, null-terminated string
        col_end = data.index(0, offset)
        col_name = data[offset:col_end].decode('utf-8')
        offset = col_end + 1
        # 4 bytes: type OID
        type_oid = struct.unpack_from('!I', data, offset)[0]
        offset += 4
        # 4 bytes: type modifier
        type_mod = struct.unpack_from('!i', data, offset)[0]
        offset += 4
        columns.append({
            'name': col_name,
            'type_oid': type_oid,
            'type_mod': type_mod,
            'is_key': bool(flags & 1),
        })

    return {
        'type': 'relation',
        'rel_id': rel_id,
        'namespace': namespace,
        'name': rel_name,
        'replica_identity': replica_identity,
        'columns': columns,
    }
```

Students implement `parse_insert`, `parse_update`, and `parse_delete` themselves. The tuple data format is shared, each column is either `n` (null), `u` (unchanged toast), or `t` followed by a 4-byte length and the value as a UTF-8 string. This is simpler than it sounds, but students need to handle it correctly.

The Insert/Update/Delete parsers all share a tuple-parsing helper:

```python
def parse_tuple_data(data: bytes, offset: int) -> tuple[dict, int]:
    """Parse a TupleData message. Returns (column_values, new_offset)."""
    n_cols = struct.unpack_from('!H', data, offset)[0]
    offset += 2
    values = []
    for _ in range(n_cols):
        col_type = chr(data[offset])
        offset += 1
        if col_type == 'n':  # null
            values.append(None)
        elif col_type == 'u':  # unchanged TOAST, not sent
            values.append('<unchanged>')
        elif col_type == 't':  # text
            length = struct.unpack_from('!I', data, offset)[0]
            offset += 4
            val = data[offset:offset + length].decode('utf-8')
            offset += length
            values.append(val)
    return values, offset
```

### Phase 2, Wiring the consumer to DuckDB (20 min)

Now students have a stream of parsed change events. The next step is applying them to DuckDB to maintain a synchronized materialized view.

The logic is straightforward:

- **INSERT** → `INSERT INTO orders VALUES (...)`
- **UPDATE** → `UPDATE orders SET ... WHERE id = ?` (or `DELETE` + `INSERT`, either works, and the delete-insert pattern is simpler for column stores)
- **DELETE** → `DELETE FROM orders WHERE id = ?`

But there's a subtlety. DuckDB (like most OLAP engines) is not optimized for row-at-a-time writes. Issuing one `INSERT` per CDC event is functional but slow. In production, you'd micro-batch: accumulate events for a short window (100ms-1s), then apply them as a batch. For this exercise, row-at-a-time is fine, the point is correctness, not throughput. But mention the micro-batch optimization because students will need it in Lesson 7.

```python
import duckdb

class DuckDBSink:
    """Applies CDC events to a DuckDB materialized view."""

    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self._ensure_schema()

    def _ensure_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id BIGINT PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                _cdc_updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)

    def apply_insert(self, values: dict):
        self.conn.execute("""
            INSERT INTO orders (id, customer_id, amount, status, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, [values['id'], values['customer_id'], values['amount'],
              values['status'], values['created_at']])

    def apply_update(self, values: dict):
        # Delete + insert is idempotent and simpler than UPDATE
        self.conn.execute("DELETE FROM orders WHERE id = ?", [values['id']])
        self.apply_insert(values)

    def apply_delete(self, identity: dict):
        self.conn.execute("DELETE FROM orders WHERE id = ?", [identity['id']])

    def get_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
```

**Key design decision:** using delete-then-insert for updates instead of `UPDATE ... SET`. This is idempotent, if the same event is applied twice, the result is the same. Idempotency matters because any CDC consumer can crash and replay events. If your apply logic isn't idempotent, you corrupt data on replay. This connects back to the idempotency discussion in Lesson 4.

### Phase 3, The main loop and LSN tracking (15 min)

Now bring it all together. The main loop reads from the replication stream, parses messages, applies changes to DuckDB, and, critically, sends feedback to Postgres confirming which LSN has been processed.

LSN feedback is how Postgres knows it can recycle WAL segments. If the consumer never sends feedback, the slot retains WAL forever and disk fills up. This is the most common CDC operational failure, and students need to understand it viscerally.

```python
def run_cdc_consumer():
    """Main CDC loop: Postgres -> parse -> DuckDB."""
    sink = DuckDBSink("materialized.duckdb")

    conn = psycopg.connect(
        "host=localhost dbname=postgres user=postgres password=postgres",
        replication=True,
        autocommit=True,
    )
    cur = conn.cursor()

    # Start replication from the slot's current position
    cur.execute(
        "START_REPLICATION SLOT orders_slot LOGICAL 0/0"
        " (proto_version '2', publication_names 'orders_pub')"
    )

    # Track relation metadata (sent before DML events)
    relations = {}
    current_tx = None
    events_applied = 0

    print("CDC consumer started. Waiting for changes...")

    while True:
        msg = cur.read_message()
        if msg is None:
            # No message available, send keepalive and continue
            cur.send_feedback()
            continue

        # msg.payload is the raw pgoutput bytes
        event = parse_pgoutput_message(msg.payload)

        if event['type'] == 'relation':
            relations[event['rel_id']] = event

        elif event['type'] == 'begin':
            current_tx = event

        elif event['type'] == 'insert':
            rel = relations[event['rel_id']]
            values = dict(zip(
                [c['name'] for c in rel['columns']],
                event['values']
            ))
            sink.apply_insert(values)
            events_applied += 1

        elif event['type'] == 'update':
            rel = relations[event['rel_id']]
            values = dict(zip(
                [c['name'] for c in rel['columns']],
                event['new_values']
            ))
            sink.apply_update(values)
            events_applied += 1

        elif event['type'] == 'delete':
            rel = relations[event['rel_id']]
            identity = dict(zip(
                [c['name'] for c in rel['columns']],
                event['old_values']
            ))
            sink.apply_delete(identity)
            events_applied += 1

        elif event['type'] == 'commit':
            # Send feedback after each committed transaction
            cur.send_feedback(flush_lsn=msg.data_start)
            if events_applied % 100 == 0:
                print(f"Applied {events_applied} events. "
                      f"DuckDB count: {sink.get_count()}")

    cur.close()
    conn.close()
```

**Critical teaching point about `send_feedback`:** the `flush_lsn` parameter tells Postgres "I've durably processed everything up to this LSN." Postgres uses this to advance the slot's `confirmed_flush_lsn` and recycle old WAL. If students call `send_feedback` before actually applying the event to DuckDB, they risk data loss, if the consumer crashes between the feedback and the DuckDB write, that event is gone. If they never call `send_feedback`, disk fills up. The correct pattern is: apply the change, then confirm the LSN. In a micro-batching consumer, confirm only after the batch is flushed.

### Phase 4, Test it (15 min)

Students run the CDC consumer in one terminal and execute DML in another:

```sql
-- Terminal 2: generate changes
INSERT INTO orders (customer_id, amount, status)
VALUES (1, 49.99, 'pending');

INSERT INTO orders (customer_id, amount, status)
VALUES (2, 149.99, 'pending');

UPDATE orders SET status = 'shipped' WHERE id = 1;

DELETE FROM orders WHERE id = 2;
```

In the consumer terminal, they should see each event arrive and be applied. Then verify in DuckDB:

```python
# In a separate python session
import duckdb
con = duckdb.connect("materialized.duckdb")
con.execute("SELECT * FROM orders").fetchall()
# Should show: order 1 with status='shipped', order 2 gone
```

Then run the load generator from Lesson 1 (at moderate TPS, say 1000 inserts/second) and watch the CDC consumer keep up. Students should observe the lag, how far behind is the consumer? Check it in Postgres:

```sql
SELECT slot_name,
       confirmed_flush_lsn,
       pg_current_wal_lsn(),
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'orders_slot';
```

If `lag_bytes` is growing, the consumer can't keep up. If it's stable, the consumer is keeping pace. This metric is the single most important operational signal for any CDC system. Debezium exposes it as a JMX metric. Kafka Connect tracks it as consumer lag. Students are seeing the raw version.

---

## Hour 3, Edge cases, failure modes, and the honest view

### Experiment A, Initial snapshot (20 min)

The replication slot only captures changes *after* it's created. If the `orders` table had 1M rows before the slot existed, those rows aren't in the change stream. This is the **initial snapshot problem**.

Students implement a snapshot: before starting the replication consumer, do a one-time bulk copy of existing data into DuckDB. The critical detail is using the replication slot's `consistent_point` (the LSN returned when the slot was created) to avoid gaps or duplicates.

The correct sequence:

1. Create the replication slot. Note the `consistent_point` LSN.
2. In a transaction with `REPEATABLE READ` isolation (to get a snapshot consistent with the slot's starting point), `SELECT * FROM orders` and bulk-load into DuckDB.
3. Start the CDC consumer from the slot's LSN.

This guarantees no gaps and no duplicates: the snapshot captures everything up to the `consistent_point`, and the stream captures everything after.

```python
def initial_snapshot(pg_conn_string: str, sink: DuckDBSink):
    """Bulk-load existing data before starting CDC."""
    conn = psycopg.connect(pg_conn_string)
    with conn.transaction():
        # REPEATABLE READ gives us a consistent snapshot
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur = conn.execute("SELECT * FROM orders")
        batch = []
        for row in cur:
            batch.append(row)
            if len(batch) >= 10000:
                sink.bulk_insert(batch)
                batch = []
        if batch:
            sink.bulk_insert(batch)
    conn.close()
    print(f"Snapshot complete: {sink.get_count()} rows loaded")
```

**Why this matters:** Debezium does exactly this. Its "snapshot" phase reads the table and captures the current LSN, then switches to streaming mode. If students understand the snapshot problem, they understand why Debezium's initial snapshot can take hours on large tables and why it's a deployment concern.

### Experiment B, Consumer crash and recovery (15 min)

Kill the CDC consumer mid-stream (Ctrl+C while the load generator is running). Wait 30 seconds. Restart the consumer.

Students should observe:

1. The consumer resumes from the last confirmed LSN, no events are lost.
2. Some events may be replayed (if the consumer applied them to DuckDB but crashed before sending feedback). This is **at-least-once delivery**, the default for almost all CDC systems.
3. DuckDB should still be consistent because the apply logic is idempotent (delete+insert pattern).

Check the WAL retention while the consumer is down:

```sql
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
FROM pg_replication_slots
WHERE slot_name = 'orders_slot';
```

Watch `retained_wal_bytes` grow. In production with high write throughput, a consumer outage of minutes can retain gigabytes of WAL. An outage of hours can fill the disk and crash Postgres. This is why replication slot monitoring is non-negotiable.

Show students `max_slot_wal_keep_size` (Postgres 13+), a safety valve that drops replication slots that retain too much WAL. It protects the database at the cost of losing CDC continuity (the consumer would need a full re-snapshot).

### Experiment C, Schema evolution (15 min)

While the consumer is running, add a column to the source table:

```sql
ALTER TABLE orders ADD COLUMN notes TEXT;
INSERT INTO orders (customer_id, amount, status, notes)
VALUES (99, 9.99, 'pending', 'rush delivery');
```

What happens? The Relation message in the `pgoutput` stream is re-sent with the updated schema. The consumer receives it, and the next Insert message includes the new column.

Students must handle this in their parser, update the `relations` dict when a new Relation message arrives for a known `rel_id`. On the DuckDB side, they need to either:

- `ALTER TABLE orders ADD COLUMN notes TEXT` dynamically, or
- Ignore unknown columns, or
- Crash and require manual intervention.

The first option is what production CDC systems (Debezium) do. Have students implement it, detect a schema change by comparing the incoming Relation message to the last known schema for that table, and issue `ALTER TABLE` in DuckDB for any new columns. Dropping columns is harder (DuckDB has data in that column), the pragmatic choice is to leave the column in DuckDB with NULLs for new rows.

This exercise makes a critical point: **CDC is a schema coupling between producer and consumer.** Schema evolution is the hardest operational problem in CDC, and it's why schema registries (Lesson 11) exist.

### Experiment D, The disk-full scenario (10 min)

Demonstrate (don't just lecture) what happens when a slot retains too much WAL. The easiest way: create a replication slot, don't consume from it, and run the load generator.

```sql
-- Create an abandoned slot
SELECT pg_create_logical_replication_slot('abandoned_slot', 'pgoutput');
```

```sql
-- Monitor WAL growth while the load generator runs
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 AS retained_mb
FROM pg_replication_slots
WHERE slot_name = 'abandoned_slot';
```

Students watch `retained_mb` climb. In a resource-constrained Docker container, this will eventually fill the disk and Postgres will refuse to accept writes, a total outage caused by a forgotten replication slot. The fix:

```sql
SELECT pg_drop_replication_slot('abandoned_slot');
```

**Rule students should memorize:** every replication slot must have a monitored consumer. An unmonitored slot is a ticking time bomb. Alert on `pg_replication_slots.confirmed_flush_lsn` falling behind `pg_current_wal_lsn()`.

### Optional expansion, Debezium in 10 minutes

For students who finish early or want to see the production approach. This is show-and-tell, not hands-on.

Provide a Docker Compose file that adds Debezium (Kafka Connect + Debezium Postgres connector) and a single Kafka broker to the existing Postgres setup:

```yaml
# docker-compose.debezium.yml (abbreviated)
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    # ... KRaft mode config (no ZooKeeper)

  connect:
    image: debezium/connect:2.5
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses

  # Postgres is already running from the earlier exercise
```

Register the connector:

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "topic.prefix": "cdc",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication"
  }
}'
```

Then consume from the Kafka topic and show the event structure:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cdc.public.orders --from-beginning --max-messages 5
```

Students see a JSON envelope with `before`, `after`, `source` (with LSN, timestamp, transaction ID), and `op` (c/u/d/r for create/update/delete/read-during-snapshot). Point out: this is exactly the information their hand-built consumer parsed from raw `pgoutput` bytes, just packaged in a structured, standard format and delivered via Kafka.

**The sentence to leave them with:** "Now you know what Debezium does. It's not magic, it's a well-engineered consumer of the same replication protocol you just implemented. The difference is it handles snapshotting, offset tracking, schema history, and fault tolerance so you don't have to. Use it in production. But now you can debug it when it breaks, because you've seen every layer."

---

## Take-home deliverable

A repository containing:

- **`cdc_consumer.py`**, the working CDC consumer that reads from Postgres logical replication and applies changes to DuckDB. Must handle INSERT, UPDATE, and DELETE. Must send LSN feedback correctly.
- **`load_generator.py`**, a script that generates INSERTs, UPDATEs, and DELETEs against the source Postgres to exercise all three code paths.
- **`verify.py`**, a script that compares the Postgres source table against the DuckDB materialized view and reports any discrepancies (missing rows, stale values, phantom rows).
- **`README.md`**, setup instructions, architecture description, and a section documenting observed lag under load (with the `pg_replication_slots` query output).
- **`CLAUDE.md`** (or `AGENTS.md`), instructions for AI coding assistants to understand and work with the codebase.

Acceptance criteria:

1. Run `load_generator.py` for 60 seconds generating mixed INSERTs/UPDATEs/DELETEs.
2. Run `cdc_consumer.py` concurrently.
3. Stop the load generator. Wait for the consumer to catch up (lag_bytes = 0).
4. Run `verify.py`, DuckDB must match Postgres exactly. Zero discrepancies.
5. Kill the consumer, run the load generator for 10 more seconds, restart the consumer. After it catches up, `verify.py` must still report zero discrepancies.

AI assistance is encouraged. Submit via PR.
