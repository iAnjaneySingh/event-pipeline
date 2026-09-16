# Event-Driven Order Pipeline (Kafka / Redpanda)

A Python producer/consumer pipeline built on Redpanda (Kafka-API-compatible —
swap the bootstrap address and this runs unmodified against real Kafka).
Built around the failure modes that actually show up running Kafka in
production, not just the publish/subscribe happy path.

## What it handles

- **Idempotent producing** — `enable.idempotence=True` so broker-level retry
  on a transient network blip can't create a duplicate write
- **Ordering per key** — events are keyed by `order_id`, so all events for one
  order land on the same partition and are processed in order
- **Manual offset commits** — the consumer only commits after an event is
  fully processed (or explicitly routed to the DLQ), so a crash mid-processing
  results in redelivery, not silent data loss
- **Idempotent consuming** — Kafka only guarantees at-least-once delivery, so
  a Redis-backed dedupe store tracks processed `event_id`s and skips repeats
  from redelivery/rebalance
- **Schema versioning** — every event carries `schema_version`, so you can
  evolve the schema without breaking consumers mid-rollout
- **Dead-letter topic** — malformed events and events that fail business-logic
  validation get routed to `orders.events.dlq` instead of blocking the
  partition or crashing the consumer
- **Consumer-group scaling** — 2 consumer replicas share the topic's
  partitions via Kafka's built-in group rebalancing

## Architecture

```
producer --(keyed by order_id)--> [orders.events topic] --> consumer group
                                                                  |
                                                    dedupe check (Redis)
                                                                  |
                                                      process_event()
                                                        /            \
                                                    success          failure /
                                                       |            malformed
                                                  commit offset          |
                                                                  [orders.events.dlq]
                                                                         |
                                                                  commit offset
```

## Run it

```bash
docker compose up --build
```

This starts Redpanda, Redis, a producer emitting ~1 event/sec, and 2 consumer
replicas. Watch the consumer logs — roughly 15% of events have a negative
amount on purpose, so you'll see the DLQ path fire.

## Run tests (no Kafka/Redis required)

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Design notes / what this demonstrates

- The difference between **"at-least-once delivery"** (what Kafka gives you)
  and **"exactly-once processing"** (what you have to build yourself, via
  idempotency) — this is the most common real-world Kafka consumer bug
- **Manual offset management** so failures don't silently drop data
- **Partitioning strategy** and why key choice matters for ordering guarantees
- **Poison-message handling** that doesn't block the whole partition
