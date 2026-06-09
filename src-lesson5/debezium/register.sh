#!/usr/bin/env bash
# Register the Debezium Postgres connector (slide 28). Run after the overlay is up.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "Waiting for Kafka Connect REST API on :8083 ..."
until curl -sf http://localhost:8083/ >/dev/null; do sleep 2; done

echo "Registering orders-connector ..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"$HERE/register-orders-connector.json" \
  http://localhost:8083/connectors | sed 's/,/,\n/g'

echo
echo "Status:"
curl -s http://localhost:8083/connectors/orders-connector/status

cat <<'EOF'

---
Watch the change events land in Kafka (one topic per table, cdc.public.orders):

  docker compose -f docker-compose.yml -f debezium/docker-compose.debezium.yml \
    exec kafka /kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic cdc.public.orders --from-beginning

Then UPDATE/DELETE a row in psql and watch the JSON events appear. Debezium just
did, automatically: a slot ('debezium_slot'), a snapshot, offset tracking, and
schema handling — the same parts you built by hand.
EOF
