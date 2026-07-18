"""Generate a controlled-rate order stream for the latency benchmark.

Each event embeds its event-time timestamp (`ts`) and the wall-clock time it was
sent to Kafka (`produced_at_ms`). The downstream pipelines compute the same
5-minute tumbling windows and record when they emit each result, so we can
measure processing and end-to-end latency.

Usage:
    uv run python src/producer.py --rate 100 --duration-seconds 600
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import BOOTSTRAP, DATA_DIR, ORDERS_TOPIC, banner


def delivery_report(err, _msg):
    if err is not None:
        print(f"delivery error: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="produce orders for L9 benchmark")
    parser.add_argument("--rate", type=int, default=100, help="events per second")
    parser.add_argument("--duration-seconds", type=int, default=600, help="how long to produce")
    parser.add_argument("--topic", default=ORDERS_TOPIC, help="target Kafka topic")
    args = parser.parse_args()

    banner(
        "Lesson 9 producer",
        f"bootstrap: {BOOTSTRAP}",
        f"topic:     {args.topic}",
        f"rate:      {args.rate} events/s",
        f"duration:  {args.duration_seconds}s (~{args.rate * args.duration_seconds:,} events)",
    )

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    })

    interval = 1.0 / args.rate
    start = time.time()
    order_id = 0
    summary = {"start_time": datetime.now(timezone.utc).isoformat(), "rate": args.rate}

    try:
        while time.time() - start < args.duration_seconds:
            produced_at_ms = int(time.time() * 1000)
            order = {
                "order_id": order_id,
                "customer_id": order_id % 50,
                "amount": round(10.0 + (order_id % 100) * 1.5, 2),
                "status": "pending",
                "ts": datetime.now(timezone.utc).isoformat(),
                "produced_at_ms": produced_at_ms,
            }
            producer.produce(
                topic=args.topic,
                key=str(order["customer_id"]).encode("utf-8"),
                value=json.dumps(order).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            order_id += 1
            if order_id % args.rate == 0:
                elapsed = time.time() - start
                print(f"  produced {order_id:,} events in {elapsed:.1f}s ({order_id / elapsed:.1f} events/s)")
            time.sleep(interval)
    finally:
        producer.flush()

    summary.update({
        "events": order_id,
        "duration_seconds": round(time.time() - start, 2),
        "effective_rate": round(order_id / (time.time() - start), 2) if time.time() > start else 0,
    })

    summary_file = DATA_DIR / "producer_summary.json"
    summary_file.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nproducer summary written to {summary_file}")


if __name__ == "__main__":
    main()
