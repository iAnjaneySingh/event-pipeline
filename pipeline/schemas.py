import time
import uuid
from pydantic import BaseModel, Field


class OrderEvent(BaseModel):
    """schema_version lets consumers handle old and new event shapes side by
    side during a rollout, instead of breaking on the first field you add."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: int = 1
    order_id: str
    customer_id: str
    amount_cents: int
    status: str  # created | paid | cancelled
    emitted_at: float = Field(default_factory=time.time)
