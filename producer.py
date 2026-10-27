#!/usr/bin/env python3
"""Publish sample order events to RabbitMQ."""

import argparse
import json
import os
import uuid

import pika


QUEUE = os.getenv("ORDER_QUEUE", "orders")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")


def publish(count: int) -> None:
    parameters = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)

    for number in range(1, count + 1):
        event = {
            "event_id": str(uuid.uuid4()),
            "order_id": f"order-{number:03d}",
            "item_id": f"item-{((number - 1) % 3) + 1:03d}",
            "quantity": number,
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(event).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"PUBLISHED event_id={event['event_id']} order_id={event['order_id']} "
              f"item_id={event['item_id']} quantity={event['quantity']}", flush=True)

    connection.close()
    print(f"Published {count} message(s) to queue={QUEUE}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=6)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")
    publish(args.count)
