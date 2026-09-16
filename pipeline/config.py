import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EVENTS = "orders.events"
TOPIC_DLQ = "orders.events.dlq"
CONSUMER_GROUP = "orders-processor"

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
DEDUPE_TTL_SECONDS = 24 * 3600
