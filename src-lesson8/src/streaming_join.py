"""Lesson 8 main: stream-static join + idempotent Postgres upsert.

Run this after `seed_customers.py` and `seed_transactions.py` are producing.
The checkpoint directory (`ckpt/join`) is the key to the kill-and-restart test.
"""

import signal

import psycopg

from config import (BOOTSTRAP, CKPT_DIR, POSTGRES_URL, DB_TABLE,
                    ProgressPump, banner, build_spark, read_customers_static,
                    read_transactions, lesson)


def write_to_postgres(batch_df, batch_id: int):
    """Write a micro-batch to Postgres using an idempotent upsert."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    conn = psycopg.connect(POSTGRES_URL)

    upsert_sql = f"""
        INSERT INTO {DB_TABLE}
            (transaction_id, customer_id, amount, currency,
             transaction_time, customer_name, customer_tier, customer_region)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            transaction_time = EXCLUDED.transaction_time,
            customer_name = EXCLUDED.customer_name,
            customer_tier = EXCLUDED.customer_tier,
            customer_region = EXCLUDED.customer_region,
            processed_at = now()
    """

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(upsert_sql, (
                row["transaction_id"],
                row["customer_id"],
                row["amount"],
                row["currency"],
                row["transaction_time"],
                row["customer_name"],
                row["customer_tier"],
                row["customer_region"],
            ))
    conn.commit()
    conn.close()
    print(f"  micro-batch {batch_id}: wrote {len(rows)} rows")


def main():
    banner("streaming_join",
           "Spark stream-static join: transactions + compacted customers",
           f"Kafka: {BOOTSTRAP}",
           f"Postgres: {POSTGRES_URL}",
           f"Checkpoint: {CKPT_DIR / 'join'}")

    spark = build_spark("lesson8-stateful-join")

    transactions = read_transactions(spark, starting="earliest")
    customers = read_customers_static(spark)

    print(f"loaded customer snapshot: {customers.count()} rows")

    enriched = (transactions
                .join(customers, on="customer_id", how="left")
                .select("transaction_id", "customer_id", "amount", "currency",
                        "transaction_time",
                        customers["name"].alias("customer_name"),
                        customers["tier"].alias("customer_tier"),
                        customers["region"].alias("customer_region")))

    query = (enriched.writeStream
             .foreachBatch(write_to_postgres)
             .option("checkpointLocation", str(CKPT_DIR / "join"))
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
        "Spark reads transactions from Kafka and a snapshot of customers from",
        "  a compacted topic. The join is stateful, and the checkpoint stores",
        "  both Kafka offsets and the intermediate state.",
        "The write to Postgres is at-least-once. The upsert makes it",
        "  effectively exactly-once: the same transaction can be written again",
        "  after a crash, but the row in Postgres will not change.",
    )


if __name__ == "__main__":
    main()
