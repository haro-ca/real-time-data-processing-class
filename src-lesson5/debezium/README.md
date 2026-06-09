# Debezium show-and-tell (optional)

Slides 27–29. This is **not** part of the hands-on build — it's the 10-minute
"here's what you just built, productionized" demo. Everything you did by hand
(slot, snapshot, LSN confirm, idempotent apply, lag) Debezium does for you; you
hand it a JSON config instead of writing a loop.

It runs against the **same** `postgres` source as the base stack, so start it
combined with the base compose file (same project + network):

```bash
# from src-lesson5/
docker compose -f docker-compose.yml -f debezium/docker-compose.debezium.yml up -d
bash debezium/register.sh
```

`register.sh` waits for Kafka Connect, POSTs `register-orders-connector.json`,
and prints the watch command. Then, in another terminal, mutate the source and
watch events land in Kafka:

```bash
docker compose exec postgres psql -U bench -d bench \
  -c "UPDATE orders SET status='shipped' WHERE id = 42;"

docker compose -f docker-compose.yml -f debezium/docker-compose.debezium.yml \
  exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic cdc.public.orders --from-beginning
```

## What to point out

- **`plugin.name: pgoutput`** — Debezium parses the *binary* protocol you skipped.
  Same WAL, different output plugin; you read `wal2json` for readability.
- **`slot.name: debezium_slot`** — its own slot, separate from your `orders_slot`.
  Two consumers = two slots = double the WAL load on the source. That's the wall
  that motivates Kafka in Lesson 6: put a durable log in the middle so many
  consumers read at their own pace without each pinning the source's WAL.
- It auto-creates a publication (`dbz_publication`) and runs an initial snapshot —
  exactly the pieces you wired in `setup_cdc.py` and `snapshot.py`.

## Teardown

```bash
docker compose -f docker-compose.yml -f debezium/docker-compose.debezium.yml down
# drop Debezium's slot so it stops retaining WAL on the source:
docker compose exec postgres psql -U bench -d bench \
  -c "SELECT pg_drop_replication_slot('debezium_slot');"
```

> Images: Debezium's quickstart trio (ZooKeeper + Kafka + Connect) for copy-paste
> reliability. Production today is KRaft (no ZooKeeper); the connector config is
> identical.
