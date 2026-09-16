import redis

from . import config


class DedupeStore:
    """Tracks which event_ids have already been processed so consumers stay
    idempotent under Kafka's at-least-once delivery guarantee (redelivery
    after a rebalance or a crash-before-commit must not double-process)."""

    def __init__(self, redis_url: str = config.REDIS_URL):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

    def already_processed(self, event_id: str) -> bool:
        return self.r.exists(f"dedupe:{event_id}") == 1

    def mark_processed(self, event_id: str):
        self.r.set(f"dedupe:{event_id}", "1", ex=config.DEDUPE_TTL_SECONDS)
