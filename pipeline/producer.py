import logging

from confluent_kafka import Producer

from . import config
from .schemas import OrderEvent

log = logging.getLogger("producer")


def _delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())


class EventProducer:
    def __init__(self, bootstrap: str = config.KAFKA_BOOTSTRAP):
        # enable.idempotence=True -> broker dedupes retried sends at the
        # partition level, so a network retry doesn't create a duplicate write
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    def publish(self, event: OrderEvent, topic: str = config.TOPIC_EVENTS):
        payload = event.model_dump_json().encode("utf-8")
        self.producer.produce(
            topic,
            key=event.order_id.encode("utf-8"),   # same key -> same partition -> preserves per-order ordering
            value=payload,
            callback=_delivery_report,
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()
