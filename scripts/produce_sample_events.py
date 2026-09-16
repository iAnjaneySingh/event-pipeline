"""Publishes sample order events continuously. Roughly 15% have a negative
amount on purpose, to exercise the consumer's DLQ path."""

import random
import time
import uuid

from pipeline.producer import EventProducer
from pipeline.schemas import OrderEvent

STATUSES = ["created", "paid", "cancelled"]


def main():
    producer = EventProducer()
    print("Producing sample order events... Ctrl+C to stop.")
    try:
        while True:
            amount = random.randint(-500, 20000) if random.random() < 0.15 else random.randint(500, 20000)
            event = OrderEvent(
                order_id=str(uuid.uuid4()),
                customer_id=f"cust_{random.randint(1, 50)}",
                amount_cents=amount,
                status=random.choice(STATUSES),
            )
            producer.publish(event)
            print(f"Published {event.event_id} order={event.order_id} amount={event.amount_cents}")
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
