# Lesson 4, Classical batch ETL and why "just move the data" is hard

Lesson 3 ended with the punchline: OLTP and OLAP optimize for opposite access patterns. That means data has to move from one to the other. The naive version of that sounds trivial, SELECT from Postgres, write to DuckDB, done. This lesson exists because that naive version breaks in every way that matters: it duplicates data on retry, it can't handle schema changes, it loses track of what it already moved, and it falls over at 3 AM with no one watching. Students need to feel this pain in raw python before they're allowed to touch an orchestrator.

## Hour 1, Theory: why moving data is a systems problem, not a scripting problem

### Module A, ETL vs ELT: a real distinction, not a marketing one

ETL (Extract, Transform, Load) transforms data *before* loading it into the target. ELT (Extract, Load, Transform) loads raw data first, then transforms it inside the target system. The difference isn't pedantic, it determines where compute happens and who owns the transformation logic.

**ETL** was the dominant paradigm when analytical targets were expensive (proprietary data warehouses billed per compute-hour). You'd clean, aggregate, and reshape data in a cheap middle layer (python, Spark, Informatica) so the warehouse only stored the final result. The downside: you've thrown away the raw data. If the business asks a new question next month, you can't go back and re-derive the answer from data you never loaded.

**ELT** became dominant when analytical storage got cheap (S3, BigQuery, Snowflake, DuckDB). You load everything raw, then transform in-place using SQL. The raw data is always available for re-processing. The downside: your analytical target must be powerful enough to run the transformations, and you're storing a lot of data you might never query.

For this lesson, the practical exercise uses a hybrid approach that mirrors what most teams actually do: extract from Postgres, transform with DuckDB (an analytical engine that's great at transformations), load the result into an analytical target. This is technically ETL, but the "T" happens in a columnar engine, so it has the performance characteristics of ELT. The labels matter less than the architecture.

### Module B, Idempotency: the only property that lets you sleep at night

An operation is **idempotent** if running it N times produces the same result as running it once. This is the single most important property of a batch pipeline, and most students will build their first pipeline without it.

Why does it matter? Because pipelines fail. The network drops mid-load. The transform runs out of memory. The target database's disk fills up. When you re-run the pipeline after a failure, what happens?

- **Without idempotency:** duplicated rows. You extracted 1M rows, loaded 500k before the crash, re-ran, and now you have 1.5M rows in the target. Or worse, you re-ran the transform on already-transformed data and got corrupted aggregates.
- **With idempotency:** the same result as if the pipeline had succeeded the first time.

There are three common strategies for achieving idempotency in batch loads:

**Strategy 1, DELETE + INSERT (partition replacement):**

```sql
-- In the target database, within a single transaction
BEGIN;
DELETE FROM analytics.daily_revenue WHERE date = '2024-01-15';
INSERT INTO analytics.daily_revenue (date, total_revenue, order_count)
    SELECT '2024-01-15', SUM(amount), COUNT(*)
    FROM staging.orders
    WHERE created_at::date = '2024-01-15';
COMMIT;
```

This is the simplest idempotent pattern. You blow away the target partition and rewrite it. Run it 10 times, you get the same result. The downside: it's expensive for large partitions, and it requires a clear partitioning key (usually date).

**Strategy 2, UPSERT (MERGE / INSERT ON CONFLICT):**

```sql
INSERT INTO analytics.daily_revenue (date, total_revenue, order_count)
VALUES ('2024-01-15', 42000.00, 1523)
ON CONFLICT (date) DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    order_count = EXCLUDED.order_count;
```

Idempotent because re-inserting the same key overwrites the existing row with the same values. Works well for dimension tables and aggregated fact tables. The downside: requires a natural key or business key, if you're using surrogate keys (auto-increment IDs), `ON CONFLICT` on which column?

**Strategy 3, Staging table + swap:**

```sql
BEGIN;
DROP TABLE IF EXISTS analytics.daily_revenue_staging;
CREATE TABLE analytics.daily_revenue_staging (LIKE analytics.daily_revenue INCLUDING ALL);
-- Load into staging table
INSERT INTO analytics.daily_revenue_staging (...) SELECT ...;
-- Atomic swap
ALTER TABLE analytics.daily_revenue RENAME TO daily_revenue_old;
ALTER TABLE analytics.daily_revenue_staging RENAME TO daily_revenue;
DROP TABLE analytics.daily_revenue_old;
COMMIT;
```

The swap is atomic (within a transaction). If anything fails before the swap, the original table is untouched. This is what dbt does under the hood for `table` materializations. The downside: you need enough space for two copies of the table, and there's a brief moment during the swap where queries might see an error (mitigated by doing the renames in a transaction, but some clients may still hiccup).

**Key insight to drive home:** idempotency isn't a feature of a tool, it's a property you design into every step of the pipeline. If any step isn't idempotent, the whole pipeline isn't idempotent. Students will prove this in the practical.

### Module C, Slowly Changing Dimensions (SCD)

This is where "just move the data" hits reality. Consider a `customers` table in the OLTP system. A customer changes their address. In the OLTP system, the row is simply updated, the old address is gone (MVCC keeps it temporarily, but that's not for analytics). In the analytical system, you often need both the old and new address because historical orders should be associated with the address at the time of the order.

**SCD Type 1, Overwrite.** Just update the analytical row. Simple, but you lose history. Fine for corrections (typo in a name), bad for actual changes (address change).

**SCD Type 2, Add a new row with versioning.**

```sql
-- Customer moved from NYC to LA on 2024-03-15
-- Before:
-- | id | name  | city | valid_from | valid_to   | is_current |
-- | 42 | Alice | NYC  | 2020-01-01 | 9999-12-31 | true       |

-- After:
-- | id | name  | city | valid_from | valid_to   | is_current |
-- | 42 | Alice | NYC  | 2020-01-01 | 2024-03-15 | false      |
-- | 42 | Alice | LA   | 2024-03-15 | 9999-12-31 | true       |
```

Now you can join orders to the customer dimension using `WHERE order_date BETWEEN valid_from AND valid_to` and get the correct address at the time of each order. This is the standard approach for dimensions where history matters.

The implementation cost is significant: every batch run must detect changes (compare source to current target rows), expire old records (set `valid_to` and `is_current = false`), and insert new versions. This is not a simple `INSERT INTO ... SELECT`, it's a stateful comparison that's easy to get wrong and hard to make idempotent.

**SCD Type 3, Add columns for previous values.** Store `current_city` and `previous_city` in the same row. Limited to one level of history. Almost never sufficient in practice, but students should know it exists.

### Module D, Schema evolution: the silent killer

Your OLTP schema will change. A developer adds a column. Another developer changes a `VARCHAR(50)` to `VARCHAR(200)`. A third drops a column that nothing "uses" anymore. Your batch pipeline breaks at 3 AM.

The categories of schema change, ranked by pain:

1. **New column added to source.** Mildly annoying, your `SELECT *` now returns an extra column the target doesn't have. If you list columns explicitly (which you should), the pipeline doesn't break but you're missing data.
2. **Column type changed.** Your pipeline casts `amount` to `NUMERIC(10,2)` but the source changed it to `NUMERIC(12,4)`. Depending on the direction, you silently lose precision or the cast fails.
3. **Column renamed.** Your pipeline references `customer_id` but it's now `cust_id`. Hard failure.
4. **Column dropped.** Your pipeline selects a column that no longer exists. Hard failure.
5. **Semantic change without structural change.** A column called `status` used to contain `['pending', 'shipped', 'delivered']` and now also contains `['cancelled', 'returned']`. No schema change detected, your downstream aggregations silently produce wrong results. This is the worst kind because nothing breaks; the data is just wrong.

How to mitigate:

- **Never `SELECT *` in a pipeline.** Always list columns explicitly. This turns silent failures into loud failures.
- **Validate schema before extraction.** Query `information_schema.columns` and compare against expected schema. Fail fast if there's a mismatch.
- **Use a schema contract.** Define the expected source schema in a config file and validate it at the start of every run. This is what data contracts and schema registries formalize.
- **Version your target tables.** Append-only staging tables with a `_loaded_at` timestamp let you re-process historical loads if a schema issue is discovered retroactively.

### Module E, Why orchestration exists

At this point, students have five problems stacking up:

1. The pipeline has multiple steps (extract, transform, load) with dependencies between them.
2. Each step can fail independently, and the pipeline must recover correctly.
3. Some steps must not run if a prior step failed.
4. The pipeline must run on a schedule (daily, hourly).
5. When it fails, someone needs to be notified, and the failure must be diagnosable.

This is exactly what orchestrators solve. A **DAG (Directed Acyclic Graph)** encodes the dependency structure: "transform depends on extract; load depends on transform." The orchestrator traverses the DAG, runs each task when its dependencies are met, retries failed tasks according to a policy, and logs everything.

**What orchestrators actually provide:**

- **Dependency resolution.** You declare "task B depends on task A" and the engine figures out the execution order. This is trivial for linear pipelines but essential when you have 50 tables with cross-dependencies.
- **Retry semantics.** Task failed? Retry it 3 times with exponential backoff. If it still fails, mark the DAG run as failed but don't retry downstream tasks.
- **Idempotent re-execution.** You can re-run a specific date's pipeline without re-running the whole history. This is called "backfilling."
- **Observability.** Every task's start time, end time, duration, logs, and status are recorded and queryable. When the CEO asks "why is yesterday's dashboard wrong?" you can trace it to "the extract step for the orders table failed at 02:17 because the source database was in maintenance mode."

**The "exactly-once in batch" problem:** orchestrators don't give you exactly-once for free. If a task succeeds (data is written to the target) but the orchestrator doesn't record the success (because the orchestrator itself crashes between the task completing and the metadata write), the next run will re-execute the task. You're back to the idempotency requirement from Module B. Orchestrators make exactly-once *easier to achieve* by giving you structured places to implement idempotent logic, but **the idempotency is still your responsibility.**

End the theory hour with this framing: **every problem you just learned about is why orchestrators exist. But orchestrators don't solve these problems, they give you a framework to solve them yourself. If your individual tasks aren't idempotent, no orchestrator saves you.**

---

## Hour 2, Practical: build a batch pipeline in raw python, then break it

### Setup (10 min)

Students reuse the OLTP Postgres from Lesson 1 (with the `orders` table populated from the load generator). They also reuse DuckDB from Lesson 3. The target is a new analytical schema, either in a separate Postgres database or in a persistent DuckDB file. Either works; DuckDB is simpler for this exercise.

Seed the source if needed, students should have at least 1M orders with realistic variation in `created_at` spanning several days. If their Lesson 1 data is stale or missing:

```python
import psycopg
import random
from datetime import datetime, timedelta

def seed_orders(conn_string: str, n_rows: int = 1_000_000):
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    customer_id INT NOT NULL,
                    amount NUMERIC(10,2) NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            # Batch insert for speed
            base_time = datetime(2024, 1, 1)
            batch_size = 10_000
            for batch_start in range(0, n_rows, batch_size):
                rows = []
                for i in range(batch_size):
                    rows.append((
                        random.randint(1, 50_000),
                        round(random.uniform(5.0, 500.0), 2),
                        random.choice(['pending', 'shipped', 'delivered']),
                        base_time + timedelta(seconds=random.randint(0, 90 * 86400))
                    ))
                cur.executemany(
                    "INSERT INTO orders (customer_id, amount, status, created_at) VALUES (%s, %s, %s, %s)",
                    rows
                )
            conn.commit()
```

The analytical target table:

```sql
-- In DuckDB (or analytical Postgres)
CREATE TABLE IF NOT EXISTS daily_revenue (
    date DATE NOT NULL,
    status TEXT NOT NULL,
    total_revenue DECIMAL(18,2) NOT NULL,
    order_count BIGINT NOT NULL,
    avg_order_value DECIMAL(10,2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (date, status)
);
```

### Phase 1, The naive pipeline (20 min)

Students write the simplest possible pipeline: extract all orders for a given date from Postgres, transform them in DuckDB (aggregate to daily revenue by status), load the result into the target. Three functions: `extract()`, `transform()`, `load()`.

```python
import psycopg
import duckdb
from datetime import date, datetime

POSTGRES_CONN = "postgresql://user:pass@localhost:5432/oltp"
DUCKDB_PATH = "analytics.duckdb"

def extract(target_date: date) -> list[dict]:
    """Extract orders for a single date from OLTP Postgres."""
    with psycopg.connect(POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, customer_id, amount, status, created_at
                FROM orders
                WHERE created_at::date = %s
            """, (target_date,))
            columns = [desc.name for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

def transform(raw_orders: list[dict], target_date: date) -> list[dict]:
    """Aggregate orders into daily revenue by status using DuckDB."""
    con = duckdb.connect()
    con.execute("CREATE TABLE raw AS SELECT * FROM raw_orders")
    # DuckDB can query python variables directly, this is one of its killer features
    result = con.execute("""
        SELECT
            ?::DATE AS date,
            status,
            SUM(amount) AS total_revenue,
            COUNT(*) AS order_count,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM raw_orders
        GROUP BY status
    """, [target_date]).fetchall()
    return [
        {
            "date": row[0], "status": row[1],
            "total_revenue": row[2], "order_count": row[3],
            "avg_order_value": row[4]
        }
        for row in result
    ]

def load(rows: list[dict]):
    """Load aggregated rows into the analytical DuckDB target."""
    con = duckdb.connect(DUCKDB_PATH)
    for row in rows:
        con.execute("""
            INSERT INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["date"], row["status"], row["total_revenue"],
              row["order_count"], row["avg_order_value"], datetime.now()))

def run_pipeline(target_date: date):
    print(f"Extracting orders for {target_date}...")
    raw = extract(target_date)
    print(f"  Extracted {len(raw)} rows")
    print(f"Transforming...")
    aggregated = transform(raw, target_date)
    print(f"  Produced {len(aggregated)} aggregated rows")
    print(f"Loading...")
    load(aggregated)
    print(f"  Done.")

if __name__ == "__main__":
    run_pipeline(date(2024, 1, 15))
```

This works on the first run. Now run it again for the same date. **Ask the students: what happened?**

Duplicated rows. The `daily_revenue` table now has two entries for every `(date, status)` combination. Run it a third time, three entries. This is the exact problem Module B warned about. The pipeline has no idempotency.

### Phase 2, Make it idempotent (20 min)

Students fix the `load()` function using one of the three strategies from Module B. The simplest for this case is DELETE + INSERT within a transaction:

```python
def load_idempotent(rows: list[dict], target_date: date):
    """Idempotent load: delete existing data for the target date, then insert."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("BEGIN TRANSACTION")
    try:
        # Delete existing rows for this date
        con.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
        # Insert new rows
        for row in rows:
            con.execute("""
                INSERT INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (row["date"], row["status"], row["total_revenue"],
                  row["order_count"], row["avg_order_value"], datetime.now()))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
```

Now run it 3 times. Check the result:

```sql
SELECT date, status, COUNT(*) as row_count FROM daily_revenue GROUP BY date, status;
```

One row per `(date, status)`. That's idempotent.

Alternatively, students can use the UPSERT approach. In DuckDB:

```sql
INSERT OR REPLACE INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
VALUES (?, ?, ?, ?, ?, ?);
```

Both are valid. The DELETE + INSERT approach is more explicit about what's happening and works even without a primary key on the target (though you should have one). The UPSERT approach is more concise but requires a primary key or unique constraint.

### Phase 3, Inject failure and recover (20 min)

This is the exercise that makes the lesson stick. Students add a deliberate failure point mid-pipeline:

```python
import random

def load_with_failure(rows: list[dict], target_date: date, fail_probability: float = 0.5):
    """Load that randomly fails mid-way through, simulating a real crash."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
        for i, row in enumerate(rows):
            # Simulate crash after inserting some rows
            if i > 0 and random.random() < fail_probability:
                raise ConnectionError(f"Simulated failure after {i} rows")
            con.execute("""
                INSERT INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (row["date"], row["status"], row["total_revenue"],
                  row["order_count"], row["avg_order_value"], datetime.now()))
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"  FAILURE: {e}, rolling back")
        raise
```

The transaction semantics save you here, if the failure happens before COMMIT, the DELETE is also rolled back. The target table is unchanged. You can safely re-run.

**But now make it harder.** What if the failure happens *between* the load and some metadata tracking step? Students must implement a retry wrapper:

```python
def run_pipeline_with_retry(target_date: date, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries} for {target_date}")
            raw = extract(target_date)
            aggregated = transform(raw, target_date)
            load_with_failure(aggregated, target_date)
            print(f"  Success on attempt {attempt}")
            return
        except Exception as e:
            print(f"  Failed: {e}")
            if attempt == max_retries:
                print(f"  Exhausted retries for {target_date}")
                raise
            print(f"  Retrying...")
```

Students run this 10 times for the same date and verify: no matter how many failures and retries occurred, the final state is always the same, one correct set of rows per `(date, status)`. **This is the deliverable standard.**

### Phase 4, Multi-date pipeline with dependency tracking (10 min)

Extend the pipeline to process multiple dates. Students add a simple watermark mechanism, a metadata table that tracks which dates have been successfully processed:

```python
def ensure_metadata_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            table_name TEXT NOT NULL,
            date DATE NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL,
            row_count BIGINT NOT NULL,
            PRIMARY KEY (table_name, date)
        )
    """)

def mark_complete(con, table_name: str, target_date: date, row_count: int):
    """Mark a date as successfully loaded, inside the same transaction as the load."""
    con.execute("""
        INSERT OR REPLACE INTO pipeline_metadata (table_name, date, loaded_at, row_count)
        VALUES (?, ?, ?, ?)
    """, (table_name, target_date, datetime.now(), row_count))

def get_last_loaded_date(con, table_name: str) -> date | None:
    result = con.execute("""
        SELECT MAX(date) FROM pipeline_metadata WHERE table_name = ?
    """, (table_name,)).fetchone()
    return result[0] if result[0] else None
```

The critical subtlety: `mark_complete()` must happen **inside the same transaction as the load**. If you mark complete after committing the load, and the process crashes between the commit and the metadata write, you'll re-process the date. If you mark complete before the load commits, you'll skip data that was never actually loaded. The metadata write and the data write must be atomic.

This is the exact same problem orchestrators solve with their metadata databases, and now students understand why the orchestrator's internal state must be tightly coupled with the pipeline's output state.

---

## Hour 3, Orchestrators: from raw code to Airflow to Dagster

### Part 1, Airflow: tasks, dependencies, retries (30 min)

Students don't build an Airflow DAG from scratch, that's ops busywork. Instead, they receive a pre-written DAG that implements the exact same pipeline they just built by hand, and they read it, annotate it, and map every Airflow concept back to the manual code they wrote.

Present this DAG:

```python
# dags/daily_revenue_pipeline.py
from airflow import DAG
from airflow.operators.python import pythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import duckdb

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

def extract(**context):
    """Extract orders for the logical date from Postgres."""
    logical_date = context["logical_date"].date()
    hook = PostgresHook(postgres_conn_id="oltp_postgres")
    records = hook.get_records(
        sql="""
            SELECT id, customer_id, amount, status, created_at
            FROM orders
            WHERE created_at::date = %s
        """,
        parameters=(logical_date,)
    )
    # Push to XCom for the next task
    context["ti"].xcom_push(key="raw_orders", value=records)
    context["ti"].xcom_push(key="row_count", value=len(records))

def transform(**context):
    """Aggregate orders into daily revenue using DuckDB."""
    logical_date = context["logical_date"].date()
    raw_orders = context["ti"].xcom_pull(key="raw_orders", task_ids="extract")

    con = duckdb.connect()
    # DuckDB can query python objects directly
    result = con.execute("""
        SELECT
            ?::DATE AS date,
            status,
            SUM(amount) AS total_revenue,
            COUNT(*) AS order_count,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM raw_orders
        GROUP BY status
    """, [logical_date]).fetchall()
    context["ti"].xcom_push(key="aggregated", value=result)

def load(**context):
    """Idempotent load into analytical DuckDB."""
    logical_date = context["logical_date"].date()
    aggregated = context["ti"].xcom_pull(key="aggregated", task_ids="transform")

    con = duckdb.connect("/data/analytics.duckdb")
    con.execute("BEGIN TRANSACTION")
    con.execute("DELETE FROM daily_revenue WHERE date = ?", (logical_date,))
    for row in aggregated:
        con.execute("""
            INSERT INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (*row, datetime.now()))
    con.execute("COMMIT")

with DAG(
    dag_id="daily_revenue_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,  # This is how you backfill, Airflow runs all missed dates
    default_args=default_args,
    max_active_runs=3,  # Limit parallelism for backfills
) as dag:

    extract_task = pythonOperator(task_id="extract", python_callable=extract)
    transform_task = pythonOperator(task_id="transform", python_callable=transform)
    load_task = pythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
```

Walk through the mapping explicitly:

| Your manual code | Airflow equivalent |
|---|---|
| `run_pipeline_with_retry()` retry loop | `retries=3, retry_delay=timedelta(minutes=5)` |
| `target_date` parameter | `context["logical_date"]`, Airflow provides this |
| Calling `extract()` then `transform()` then `load()` in sequence | `extract_task >> transform_task >> load_task` (the `>>` operator defines the DAG edge) |
| `pipeline_metadata` table tracking loaded dates | Airflow's internal metadata DB, it knows which DAG runs succeeded |
| Re-running a failed date manually | "Clear" the failed task in the Airflow UI, it re-runs just that task |
| Looping over multiple dates | `catchup=True`, Airflow generates a DAG run per schedule interval since `start_date` |

**What Airflow adds that you couldn't easily do yourself:**

- A web UI showing every DAG run, task status, duration, and logs
- Alerting on failure (email, Slack, PagerDuty integrations)
- Backfill logic, run the pipeline for 90 historical dates in parallel with controlled concurrency
- Sensor tasks, "wait until this S3 file exists, then proceed"
- Cross-DAG dependencies, "this DAG can't run until that other DAG's daily run succeeds"

**What Airflow doesn't add:**

- Idempotency. Notice the `load()` function is identical to the manual version, DELETE + INSERT in a transaction. Airflow didn't make it idempotent; the student did.
- Data quality checks. Airflow runs tasks; it doesn't validate the output.
- Schema evolution handling. That's still your problem.

**Call out the known pain points honestly.** Airflow's scheduler is process-based and notoriously heavy on the metadata database. XCom (the mechanism for passing data between tasks) serializes to the metadata database, passing 1M rows through XCom is a terrible idea (use intermediate storage instead: S3, a staging table, a Parquet file). The DAG definition file is executed by the scheduler on every heartbeat just to parse the DAG structure, so import-time side effects will ruin your day. These are real operational issues that have driven teams to alternatives.

### Part 2, Dagster: assets, not tasks (30 min)

Present the same pipeline as a Dagster asset pipeline:

```python
# dagster_pipeline/assets.py
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import psycopg
import duckdb
from datetime import datetime

@asset(
    description="Raw orders extracted from OLTP Postgres for a single day",
    metadata={"source": "oltp_postgres", "table": "orders"},
)
def raw_daily_orders(context: AssetExecutionContext) -> list[dict]:
    """Extract a day's orders from the OLTP source."""
    target_date = context.partition_key  # Dagster manages partitioning natively
    with psycopg.connect("postgresql://user:pass@localhost:5432/oltp") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, customer_id, amount, status, created_at
                FROM orders WHERE created_at::date = %s
            """, (target_date,))
            columns = [desc.name for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    context.log.info(f"Extracted {len(rows)} orders for {target_date}")
    return rows

@asset(
    description="Daily revenue aggregated by status",
    deps=["raw_daily_orders"],  # Dagster infers the dependency from the asset graph
)
def daily_revenue(context: AssetExecutionContext, raw_daily_orders: list[dict]) -> MaterializeResult:
    """Transform and load: aggregate orders and write to analytical target."""
    target_date = context.partition_key
    con_mem = duckdb.connect()
    aggregated = con_mem.execute("""
        SELECT
            ?::DATE AS date,
            status,
            SUM(amount) AS total_revenue,
            COUNT(*) AS order_count,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM raw_daily_orders
        GROUP BY status
    """, [target_date]).fetchall()

    # Idempotent load
    con_disk = duckdb.connect("/data/analytics.duckdb")
    con_disk.execute("BEGIN TRANSACTION")
    con_disk.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
    for row in aggregated:
        con_disk.execute("""
            INSERT INTO daily_revenue (date, status, total_revenue, order_count, avg_order_value, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (*row, datetime.now()))
    con_disk.execute("COMMIT")

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(aggregated)),
            "preview": MetadataValue.md(
                f"| status | revenue | orders |\n|---|---|---|\n"
                + "\n".join(f"| {r[1]} | {r[2]} | {r[3]} |" for r in aggregated)
            ),
        }
    )
```

Now highlight the philosophical shift. This is the important part, not the syntax, but the mental model:

**Airflow thinks in tasks.** A DAG is a sequence of *actions*: "extract, then transform, then load." The question Airflow answers is: "did these tasks run successfully today?"

**Dagster thinks in assets.** The pipeline is a graph of *data artifacts*: "`daily_revenue` depends on `raw_daily_orders`." The question Dagster answers is: "is `daily_revenue` up to date?" This is a fundamentally different question. Dagster knows what data exists, when it was last materialized, and whether it's stale because an upstream asset was rematerialized.

| Concept | Airflow | Dagster |
|---|---|---|
| Unit of work | Task (an action to perform) | Asset (a data artifact to produce) |
| Dependencies | "run task B after task A" | "`asset B` is derived from `asset A`" |
| Backfill | Re-run a DAG run for a past date | Re-materialize an asset partition |
| Observability | "task X succeeded at 02:15" | "`daily_revenue` for 2024-01-15 was materialized at 02:15 using `raw_daily_orders` from 02:12" |
| Lineage | Implicit (you infer it from task order) | Explicit (the asset graph is the lineage) |
| Testing | Run the DAG in a test environment | Materialize a single asset with mock inputs |

**The practical consequence:** in Airflow, if you want to know "is yesterday's revenue table correct?", you check whether yesterday's DAG run succeeded. But what if the DAG succeeded and then someone re-ran the extract step for that date? The load step wasn't re-triggered, the revenue table is now stale relative to the re-extracted data, and Airflow doesn't know. In Dagster, re-materializing `raw_daily_orders` automatically marks `daily_revenue` as stale, and you can configure it to auto-rematerialize downstream assets.

**Be fair about the tradeoffs.** Dagster's asset model is elegant but newer. Airflow has a massive ecosystem, thousands of provider packages, battle-tested at every scale. Many teams are on Airflow and migrating isn't free. Dagster's "right" way to do things can feel opinionated if your pipeline doesn't fit the asset model cleanly (e.g., pipelines that are pure side effects, "send an email", aren't naturally assets). Students should know both because they'll encounter both.

### Wrap-up (5 min)

The key takeaway for Lesson 4, stated explicitly:

Moving data between systems is a systems engineering problem, not a scripting problem. The hard parts are idempotency, failure recovery, schema evolution, and historical tracking, none of which are solved by any framework. Frameworks like Airflow and Dagster give you structure and observability, but the correctness guarantees come from how you write the individual steps. If your `load()` function isn't idempotent, Airflow's retries will happily create duplicates for you, three times, with exponential backoff.

Lesson 5 will change the paradigm entirely: instead of periodically asking "what changed?" (batch ETL), you'll receive a continuous stream of changes as they happen (CDC). But CDC doesn't eliminate the problems from today, it reframes them. Idempotency, schema evolution, and exactly-once semantics all return, in harder forms.

---

## Take-home deliverable

A GitHub repository containing:

- **Pipeline code** (raw python, no orchestration framework) that extracts from OLTP Postgres, transforms with DuckDB, and loads into an analytical target.
- **The pipeline must be provably idempotent.** Include a test script or Makefile target that runs the pipeline 3 times for the same date and then queries the target to prove the result is identical each time. The proof should print row counts and checksums.
- **A failure injection mode** that randomly crashes the pipeline mid-load, and a retry wrapper that recovers correctly.
- **A `README.md`** explaining the idempotency strategy chosen, what failure modes are handled, and what would break if the source schema changed.
- **An `AGENTS.md` (or `CLAUDE.md`)** describing the project structure, how to run the pipeline, and any context an AI coding assistant would need to modify the code correctly.

Submitted as a pull request. AI assistance is encouraged, but the student must be able to explain every line of the idempotency logic in the PR review. If you used an AI to generate the DELETE + INSERT pattern but can't explain why the DELETE and INSERT must be in the same transaction, you've missed the point.
