from pipeline.schemas import OrderEvent


def test_order_event_defaults():
    event = OrderEvent(order_id="o1", customer_id="c1", amount_cents=1000, status="created")
    assert event.schema_version == 1
    assert event.event_id  # auto-generated UUID
    assert event.emitted_at > 0


def test_order_event_roundtrip_json():
    event = OrderEvent(order_id="o1", customer_id="c1", amount_cents=1000, status="paid")
    raw = event.model_dump_json()
    restored = OrderEvent.model_validate_json(raw)
    assert restored == event
