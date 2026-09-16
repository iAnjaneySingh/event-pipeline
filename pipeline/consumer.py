import logging

from confluent_kafka import Consumer, KafkaError, Producer

from . import config
from .schemas import OrderEvent
from .dedupe import DedupeStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")


def process_event(event: OrderEvent):
    """Business logic goes here. Raise to trigger DLQ routing."""
    if event.amount_cents < 0:
        raise ValueError(f"Negative amount in event {event.event_id}")
    log.info(
        "Processed order %s (%s) for customer %s — $%.2f",
        event.order_id, event.status, event.customer_id, event.amount_cents / 100,
    )


def run():
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.CONSUMER_GROUP,
        "enable.auto.commit": False,     # manual commit: only advance the offset after
        "auto.offset.reset": "earliest", # we've actually finished processing (or DLQ'd) the message
    })
    dlq_producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    dedupe = DedupeStore()

    consumer.subscribe([config.TOPIC_EVENTS])
    log.info("Consumer started, group=%s, topic=%s", config.CONSUMER_GROUP, config.TOPIC_EVENTS)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            try:
                event = OrderEvent.model_validate_json(msg.value())
            except Exception as exc:
                log.error("Malformed event, routing to DLQ: %s", exc)
                dlq_producer.produce(config.TOPIC_DLQ, value=msg.value())
                dlq_producer.poll(0)
                consumer.commit(msg)
                continue

            if dedupe.already_processed(event.event_id):
                log.info("Skipping duplicate event %s (already processed)", event.event_id)
                consumer.commit(msg)
                continue

            try:
                process_event(event)
                dedupe.mark_processed(event.event_id)
                consumer.commit(msg)   # commit only after processing succeeds
            except Exception as exc:
                log.warning("Processing failed for event %s: %s — routing to DLQ", event.event_id, exc)
                dlq_producer.produce(config.TOPIC_DLQ, value=msg.value())
                dlq_producer.poll(0)
                consumer.commit(msg)   # DLQ owns retry/inspection from here; don't block the partition
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    run()
