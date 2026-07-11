"""Lesson 8 phase 5: a genuinely stateful aggregation.

The stream-static join proves the *sink* can absorb duplicate writes. This job
proves the *state store* survives: per-customer per-minute running aggregates
live inside the Spark checkpoint between micro-batches. After a `kill -9`, the
restarted job must resume mid-window and keep the sums exact — an upsert alone
cannot repair a wrong sum, only recovered state can.

Amounts are cast to DECIMAL(12,2) before summing so the streaming sums match a
batch recomputation from Postgres exactly (no float drift).
"""

import signal

import psycopg
from pyspark.sql.functions import col, count as f_count, sum as f_sum, window

from config import (BOOTSTRAP, CKPT_DIR, POSTGRES_URL, ProgressPump, banner,
                    build_spark, lesson, read_transactions)

AGG_TABLE = "customer_activity"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {AGG_TABLE} (
        customer_id VARCHAR(64) NOT NULL,
        window_start TIMESTAMPTZ NOT NULL,
        window_end TIMESTAMPTZ NOT NULL,
        txn_count BIGINT NOT NULL,
        total_amount NUMERIC(14, 2) NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (customer_id, window_start)
    )
"""


def ensure_table():
    with psycopg.connect(POSTGRES_URL) as conn:
        conn.execute(DDL)
        conn.commit()


def write_aggregates(batch_df, batch_id: int):
    """Upsert the changed window aggregates for this micro-batch."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    upsert_sql = f"""
        INSERT INTO {AGG_TABLE}
            (customer_id, window_start, window_end, txn_count, total_amount)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id, window_start) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            txn_count = EXCLUDED.txn_count,
            total_amount = EXCLUDED.total_amount,
            updated_at = now()
    """
    with psycopg.connect(POSTGRES_URL) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(upsert_sql, (
                    row["customer_id"],
                    row["window_start"],
                    row["window_end"],
                    row["txn_count"],
                    row["total_amount"],
                ))
        conn.commit()
    print(f"  micro-batch {batch_id}: upserted {len(rows)} window aggregates")


def main():
    banner("streaming_aggregate",
           "Stateful windowed aggregation: per-customer per-minute rollups",
           f"Kafka: {BOOTSTRAP}",
           f"Postgres: {POSTGRES_URL} (table: {AGG_TABLE})",
           f"Checkpoint: {CKPT_DIR / 'aggregate'}",
           "State lives in the checkpoint. kill -9 me and the sums stay exact.")

    ensure_table()
    spark = build_spark("lesson8-stateful-aggregate")

    transactions = read_transactions(spark, starting="earliest")

    aggregates = (transactions
                  .withColumn("amount", col("amount").cast("decimal(12,2)"))
                  .withWatermark("transaction_time", "30 seconds")
                  .groupBy(window("transaction_time", "1 minute"),
                           "customer_id")
                  .agg(f_count("*").alias("txn_count"),
                       f_sum("amount").alias("total_amount"))
                  .select(col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          "customer_id", "txn_count", "total_amount"))

    query = (aggregates.writeStream
             .outputMode("update")
             .foreachBatch(write_aggregates)
             .option("checkpointLocation", str(CKPT_DIR / "aggregate"))
             .trigger(processingTime="10 seconds")
             .start())

    pump = ProgressPump(query, echo=True, interval=2.0)
    pump.start()

    def stop(signum, frame):
        print("\n  stopping query...")
        query.stop()
        pump.stop()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    query.awaitTermination()
    pump.stop()

    lesson(
        "Unlike the stream-static join, this query is truly stateful: the",
        "  running per-customer window sums live in the state store, and the",
        "  'State: N rows' metric above is finally non-zero.",
        "The checkpoint commits offsets and state versions together. After a",
        "  kill -9, Spark restores both, replays the in-flight micro-batch,",
        "  and the recomputed aggregates land on the same primary keys.",
        "The upsert makes re-delivery harmless; the state store makes the",
        "  numbers *correct*. You need both.",
    )


if __name__ == "__main__":
    main()
