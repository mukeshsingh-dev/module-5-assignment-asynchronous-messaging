#!/usr/bin/env python3
"""Consume order events with manual ACKs, redelivery, and idempotency."""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

import pika


QUEUE = os.getenv("ORDER_QUEUE", "orders")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
PROCESSED_FILE = Path(os.getenv("PROCESSED_FILE", "processed_events.txt"))


def already_processed(event_id: str) -> bool:
    """Atomically check/add an event id so separate workers share the ledger."""
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PROCESSED_FILE.open("a+", encoding="utf-8") as ledger:
        import fcntl

        fcntl.flock(ledger.fileno(), fcntl.LOCK_EX)
        ledger.seek(0)
        seen = {line.strip() for line in ledger if line.strip()}
        if event_id in seen:
            return True
        ledger.write(event_id + "\n")
        ledger.flush()
        os.fsync(ledger.fileno())
        return False


def run(name: str, crash_event: str | None, process_seconds: float) -> None:
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    print(f"[{name}] READY queue={QUEUE} pid={os.getpid()}", flush=True)

    def stop(_signum, _frame):
        print(f"[{name}] STOPPING", flush=True)
        connection.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def on_message(ch, method, _properties, body):
        event = json.loads(body.decode())
        event_id = event["event_id"]
        print(f"[{name}] RECEIVED delivery_tag={method.delivery_tag} "
              f"redelivered={method.redelivered} event_id={event_id} "
              f"order_id={event['order_id']}", flush=True)

        duplicate = already_processed(event_id)
        if duplicate:
            print(f"[{name}] DUPLICATE event_id={event_id}; business action skipped", flush=True)
        else:
            print(f"[{name}] PROCESSING order_id={event['order_id']} "
                  f"item_id={event['item_id']} quantity={event['quantity']}", flush=True)
            time.sleep(process_seconds)
            print(f"[{name}] BUSINESS ACTION COMPLETE event_id={event_id}", flush=True)

        if crash_event and event_id == crash_event:
            print(f"[{name}] SIMULATED FAILURE before ACK event_id={event_id}; "
                  "closing without acknowledgement", flush=True)
            connection.close()
            os._exit(2)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{name}] ACK event_id={event_id}", flush=True)

    try:
        channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
        channel.start_consuming()
    except pika.exceptions.AMQPError as error:
        print(f"[{name}] connection ended: {error}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="worker")
    parser.add_argument("--crash-event", help="event_id to fail after processing but before ACK")
    parser.add_argument("--process-seconds", type=float, default=0.5)
    args = parser.parse_args()
    run(args.name, args.crash_event, args.process_seconds)
